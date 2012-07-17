using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ServiceBroker.Net {
    public static class ServiceBrokerWrapper {

        public static Guid BeginConversation(IDbTransaction transaction, string initiatorServiceName, string targetServiceName, string messageContractName) {
            return BeginConversationInternal(transaction, initiatorServiceName, targetServiceName, messageContractName, null, null);
        }

        public static Guid BeginConversation(IDbTransaction transaction, string initiatorServiceName, string targetServiceName, string messageContractName, int lifetime) {
            return BeginConversationInternal(transaction, initiatorServiceName, targetServiceName, messageContractName, lifetime, null);
        }

        public static Guid BeginConversation(IDbTransaction transaction, string initiatorServiceName, string targetServiceName, string messageContractName, bool encryption) {
            return BeginConversationInternal(transaction, initiatorServiceName, targetServiceName, messageContractName, null, encryption);
        }

        public static Guid BeginConversation(IDbTransaction transaction, string initiatorServiceName, string targetServiceName, string messageContractName, int lifetime, bool encryption) {
            return BeginConversationInternal(transaction, initiatorServiceName, targetServiceName, messageContractName, lifetime, encryption);
        }

        public static void ForceEndConversation(IDbTransaction transaction, Guid conversationHandle) {
            ForceEndConversation(transaction, conversationHandle, false);
        }

        public static void ForceEndConversation(IDbTransaction transaction, Guid conversationHandle, bool withCleanup) {
            ForceEndConversationInternal(transaction, conversationHandle, false, null, null, withCleanup);
        }

        public static void ForceEndConversation(IDbTransaction transaction, Guid conversationHandle, int errorCode, string errorDescription) {
            ForceEndConversationInternal(transaction, conversationHandle, true, errorCode, errorDescription, false);
        }

        public static void EndConversation(IDbTransaction transaction, Guid conversationHandle, string queueName, int errorCode, string errorDescription) {
            EndConversationInternal(transaction, conversationHandle, queueName, true, errorCode, errorDescription, false);
        }

        public static void EndConversation(IDbTransaction transaction, Guid conversationHandle, string queueName) {
            EndConversation(transaction, conversationHandle, queueName, false);
        }

        public static void EndConversation(IDbTransaction transaction, Guid conversationHandle, string queueName, bool withCleanup) {
            EndConversationInternal(transaction, conversationHandle, queueName, false, null, null, withCleanup);
        }

        public static void Send(IDbTransaction transaction, Guid conversationHandle, string messageType) {
            Send(transaction, conversationHandle, messageType, null);
        }

        public static void Send(IDbTransaction transaction, Guid conversationHandle, string messageType, byte[] body) {
            SendInternal(transaction, conversationHandle, messageType, body);
        }

        public static IEnumerable<Message> Receive(IDbTransaction transaction, string queueName, int batchSize = 1) {
            return ReceiveInternal(transaction, queueName, null, false, null, batchSize);
        }

        public static IEnumerable<Message> Receive(IDbTransaction transaction, string queueName, Guid conversationHandle, int batchSize = 1) {
            return ReceiveInternal(transaction, queueName, conversationHandle, false, null, batchSize);
        }

        public static IEnumerable<Message> WaitAndReceive(IDbTransaction transaction, string queueName, int waitTimeout, int batchSize = 1) {
            return ReceiveInternal(transaction, queueName, null, true, waitTimeout, batchSize);
        }

        public static IEnumerable<Message> WaitAndReceive(IDbTransaction transaction, string queueName, Guid conversationHandle, int waitTimeout, int batchSize = 1) {
            return ReceiveInternal(transaction, queueName, conversationHandle, true, waitTimeout, batchSize);
        }

        public static int QueryMessageCount(IDbTransaction transaction, string queueName, string messageContractName) {
            return QueryMessageCountInternal(transaction, queueName, messageContractName);
        }

        private static Guid BeginConversationInternal(IDbTransaction transaction, string initiatorServiceName, string targetServiceName, string messageContractName, int? lifetime, bool? encryption) {
            EnsureSqlTransaction(transaction);
            var cmd = (SqlCommand) transaction.Connection.CreateCommand();
            var query = new StringBuilder();

            query.Append("BEGIN DIALOG @ch FROM SERVICE " + initiatorServiceName + " TO SERVICE @ts ON CONTRACT @cn WITH ENCRYPTION = ");

            if (encryption.HasValue && encryption.Value)
                query.Append("ON ");
            else
                query.Append("OFF ");

            if (lifetime.HasValue && lifetime.Value > 0) {
                query.Append(", LIFETIME = ");
                query.Append(lifetime.Value);
                query.Append(' ');
            }

            var param = cmd.Parameters.Add("@ch", SqlDbType.UniqueIdentifier);
            param.Direction = ParameterDirection.Output;
            param = cmd.Parameters.Add("@ts", SqlDbType.NVarChar, 256);
            param.Value = targetServiceName;
            param = cmd.Parameters.Add("@cn", SqlDbType.NVarChar, 128);
            param.Value = messageContractName;

            cmd.CommandText = query.ToString();
            cmd.Transaction = (SqlTransaction) transaction;
            var count = cmd.ExecuteNonQuery();

            var handleParam = cmd.Parameters["@ch"];
            return (Guid) handleParam.Value;
        }

        private static void EndConversationInternal(IDbTransaction transaction, Guid conversationHandle, string queueName, bool withError, int? errorCode, string errorDescription, bool withCleanup) {
            EnsureSqlTransaction(transaction);
            var cmd = (SqlCommand) transaction.Connection.CreateCommand();

            var query = new StringBuilder();
            query.Append("IF NOT EXISTS (SELECT COUNT(*) FROM ");
            query.Append(queueName);
            query.Append(" WHERE conversation_handle = @ch)");
            query.Append(" END CONVERSATION @ch");

            var pCh = cmd.Parameters.Add("@ch", SqlDbType.UniqueIdentifier);
            pCh.Value = conversationHandle;

            if (withError) {
                query.Append(" WITH ERROR = @ec DESCRIPTION = @desc");

                var pEc = cmd.Parameters.Add("@ec", SqlDbType.Int);
                pEc.Value = errorCode;

                var pDesc = cmd.Parameters.Add("@desc", SqlDbType.NVarChar, 255);
                pDesc.Value = errorDescription;
            } else if (withCleanup) {
                query.Append(" WITH CLEANUP");
            }
          
            cmd.CommandText = query.ToString();
            cmd.Transaction = (SqlTransaction) transaction;
            var count = cmd.ExecuteNonQuery();
        }

        private static void ForceEndConversationInternal(IDbTransaction transaction, Guid conversationHandle, bool withError, int? errorCode, string errorDescription, bool withCleanup) {
            EnsureSqlTransaction(transaction);
            var cmd = (SqlCommand) transaction.Connection.CreateCommand();

            cmd.CommandText = "END CONVERSATION @ch";
            var param = cmd.Parameters.Add("@ch", SqlDbType.UniqueIdentifier);
            param.Value = conversationHandle;

            if (withError) {
                cmd.CommandText += " WITH ERROR = @ec DESCRIPTION = @desc";
                param = cmd.Parameters.Add("@ec", SqlDbType.Int);
                param.Value = errorCode;
                param = cmd.Parameters.Add("@desc", SqlDbType.NVarChar, 255);
                param.Value = errorDescription;
            } else if (withCleanup) {
                cmd.CommandText += " WITH CLEANUP";
            }

            cmd.Transaction = (SqlTransaction) transaction;
            var count = cmd.ExecuteNonQuery();
        }

        private static void SendInternal(IDbTransaction transaction, Guid conversationHandle, string messageType, byte[] body) {
            EnsureSqlTransaction(transaction);
            var cmd = (SqlCommand) transaction.Connection.CreateCommand();

            var query = "SEND ON CONVERSATION @ch MESSAGE TYPE @mt ";
            var param = cmd.Parameters.Add("@ch", SqlDbType.UniqueIdentifier);
            param.Value = conversationHandle;
            param = cmd.Parameters.Add("@mt", SqlDbType.NVarChar, 255);
            param.Value = messageType;

            if (body != null && body.Length > 0) {
                query += " (@msg)";
                param = cmd.Parameters.Add("@msg", SqlDbType.VarBinary, -1);
                param.Value = body;
            }

            cmd.CommandText = query;
            cmd.Transaction = (SqlTransaction) transaction;
            var count = cmd.ExecuteNonQuery();
        }

        private static IEnumerable<Message> ReceiveInternal(IDbTransaction transaction, string queueName, Guid? conversationHandle, bool wait, int? waitTimeout, int batchSize) {
            EnsureSqlTransaction(transaction);
            var cmd = (SqlCommand) transaction.Connection.CreateCommand();

            var query = new StringBuilder();

            if (wait && waitTimeout.HasValue && waitTimeout.Value > 0)
                query.Append("WAITFOR(");

            query.Append("RECEIVE TOP(@num)");
            var pNum = cmd.Parameters.Add("@num", SqlDbType.Int);
            pNum.Value = batchSize;

            query.Append(" conversation_group_id, conversation_handle, " +
                         "message_sequence_number, service_name, service_contract_name, " +
                         "message_type_name, validation, message_body " +
                         "FROM ");
            query.Append(queueName);

            if (conversationHandle.HasValue && conversationHandle.Value != Guid.Empty) {
                query.Append(" WHERE conversation_handle = @ch");
                var pCh = cmd.Parameters.Add("@ch", SqlDbType.UniqueIdentifier);
                pCh.Value = conversationHandle.Value;
            }

            if (wait && waitTimeout.HasValue && waitTimeout.Value > 0) {
                query.Append("), TIMEOUT @to");
                var pTo = cmd.Parameters.Add("@to", SqlDbType.Int);
                pTo.Value = waitTimeout.Value;
                cmd.CommandTimeout = 0;
            }

            cmd.CommandText = query.ToString();
            cmd.Transaction = (SqlTransaction) transaction;

            IList<Message> messages;
            using (var dataReader = cmd.ExecuteReader()) {
                messages = new List<Message>();
                while (dataReader.Read()) {
                    messages.Add(Message.Load(dataReader));
                }
                dataReader.Close();
            }

            return messages;
        }

        private static int QueryMessageCountInternal(IDbTransaction transaction, string queueName, string messageContractName) {
            EnsureSqlTransaction(transaction);
            var cmd = (SqlCommand) transaction.Connection.CreateCommand();

            cmd.CommandText = "SELECT COUNT(*) FROM " + queueName + " WHERE message_type_name = @messageContractName";
            var param = cmd.Parameters.Add("@messageContractName", SqlDbType.NVarChar, 128);
            param.Value = messageContractName;
            cmd.Transaction = (SqlTransaction) transaction;

            return (int)cmd.ExecuteScalar();
        }

        private static void EnsureSqlTransaction(IDbTransaction transaction) {
            if (!(transaction is SqlTransaction))
                throw new ArgumentException("Only SqlClient is supported", "transaction");
        }
    }
}
