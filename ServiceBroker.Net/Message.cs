using System;
using System.Data.SqlClient;
using System.IO;

namespace ServiceBroker.Net {
    public class Message {

        public const string EventNotificationType = "http://schemas.microsoft.com/SQL/Notifications/EventNotification";
        public const string QueryNotificationType = "http://schemas.microsoft.com/SQL/Notifications/QueryNotification";
        public const string DialogTimerType       = "http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer";
        public const string EndDialogType         = "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog";
        public const string ErrorType             = "http://schemas.microsoft.com/SQL/ServiceBroker/Error";

        internal static Message Load(SqlDataReader reader) {
            var message = new Message
                              {
                                  ConversationGroupId = reader.GetGuid(0),
                                  ConversationHandle = reader.GetGuid(1),
                                  MessageSequenceNumber = reader.GetInt64(2),
                                  ServiceName = reader.GetString(3),
                                  ServiceContractName = reader.GetString(4),
                                  MessageTypeName = reader.GetString(5),
                                  Body = reader.IsDBNull(7) ? new byte[0] : reader.GetSqlBytes(7).Buffer
                              };
            return message;
        }

        public Guid ConversationGroupId { get; private set; }
        public Guid ConversationHandle { get; private set; }
        public long MessageSequenceNumber { get; private set; }
        public string ServiceName { get; private set; }
        public string ServiceContractName { get; private set; }
        public string MessageTypeName { get; private set; }
        public byte[] Body { get; private set; }
        public Stream BodyStream { get { return new MemoryStream(Body); } }

        private Message() { }
    }
}
