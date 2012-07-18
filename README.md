ServiceBroker.Net
========

A .NET wrapper API for SQL Server Service Broker. (SSSB)

About
-----

SQL Server Service Broker is a centralized message queuing subsystem that is provided by SQL Server. It uses logical **services** which accept messages and store them in a **queue**. Applications for that logical service can receive messages from the queue. Services are tied to **message contracts** which define which **message types** the service is able to send and/or receive.

Usage
-----

    var connectionString = "Data Source=.\SQLEXPRESS;Initial Catalog=SampleDB;Integrated Security=true";
    var inputQueueName = "SampleQueue";
    var receiveTimeout = 10000;
    var receiveLimit = 50;
    
    IEnumerable<Message> messages = null;
    var xman = new ServiceBrokerTransactionManager(connectionString);
    xman.RunInTransaction(
        transaction =>
        { 
            messages = ServiceBrokerWrapper.WaitAndReceive(transaction, inputQueueName, receiveTimeout, receiveLimit);
        });
        
    // Do something with `messages`

Service Broker Automatic Poison Message Detection
-------------------------------------------------

SQL Service Broker includes an automatic poison message detection mechanism, which is always on by default. It works by detecting rolled back transactions that occur when a RECEIVE is used on a queue. If it detects 5 consecutive rollbacks for a queue (such as retrying an errored message), then that queue is DISABLED. This requires an administrator to ENABLE the queue before messages can be received again by the application.

Contact
-------

Please e-mail **ralmoritz[at]gmail.com** for feedback, comments, or questions.
