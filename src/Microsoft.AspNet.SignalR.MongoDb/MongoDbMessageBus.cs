// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BookSleeve;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.Tracing;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Microsoft.AspNet.SignalR.MongoDb
{
    /// <summary>
    /// Uses Redis pub-sub to scale-out SignalR applications in web farms.
    /// </summary>
    public class MongoDbMessageBus : ScaleoutMessageBus
    {
        
        private readonly string _key;
        private readonly Func<MongoClient> _connectionFactory;
        private readonly TraceSource _trace;

        private MongoClient _connection;
        private MongoDatabase _database;
        private int _state;
        private readonly CancellationTokenSource _tailableCursorCancellation = new CancellationTokenSource();
        private readonly object _callbackLock = new object();
        private MongoServer _server;

        [SuppressMessage("Microsoft.Usage", "CA2214:DoNotCallOverridableMethodsInConstructors", Justification = "Reviewed")]
        public MongoDbMessageBus(IDependencyResolver resolver, MongoDbScaleoutConfiguration configuration)
            : base(resolver, configuration)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }

            _connectionFactory = configuration.ConnectionFactory;
            _key = configuration.EventKey;

            var traceManager = resolver.Resolve<ITraceManager>();
            _trace = traceManager["SignalR." + typeof(MongoDbMessageBus).Name];

            ReconnectDelay = TimeSpan.FromSeconds(2);
            ConnectWithRetry();
        }

        public TimeSpan ReconnectDelay { get; set; }

        protected override Task Send(int streamIndex, IList<Message> messages)
        {
            var tcs = new TaskCompletionSource<object>();

            SendImpl(messages, tcs);

            return tcs.Task;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                var oldState = Interlocked.Exchange(ref _state, State.Disposing);

                switch (oldState)
                {
                    case State.Connected:
                        Shutdown();
                        break;
                    case State.Closed:
                    case State.Disposing:
                        // No-op
                        break;
                    case State.Disposed:
                        Interlocked.Exchange(ref _state, State.Disposed);
                        break;
                    default:
                        break;
                }
            }

            base.Dispose(disposing);
        }

        private void Shutdown()
        {
            _trace.TraceInformation("Shutdown()");
            _tailableCursorCancellation.Cancel();

            Interlocked.Exchange(ref _state, State.Disposed);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "The exception is set in the tcs")]
        private void SendImpl(IList<Message> messages, TaskCompletionSource<object> tcs)
        {
            go:

            _database = _server.GetDatabase("SignalR");
            MongoCollection<BsonDocument> collection = _database.GetCollection("LatestId");

            const long incVal = 1;
            var newId =
                collection.FindAndModify(Query.Null, SortBy.Null, Update.Inc("LatestId", incVal))
                          .GetModifiedDocumentAs<BsonDocument>()["LatestId"].AsInt64;

            try
            {

                _trace.TraceVerbose("CreateTransaction({0})", newId);

                Task<bool> transactionTask = ExecuteTransaction(newId, messages);

                if (transactionTask.IsCompleted)
                {
                    transactionTask.Wait();

                    bool success = transactionTask.Result;

                    if (success)
                    {
                        OnTransactionComplete(success, messages, tcs);
                    }
                    else
                    {
                        _trace.TraceVerbose("Transaction failed. Retrying...");

                        _trace.TraceVerbose("Transaction disposed");

                        goto go;
                    }
                }
                else
                {
                    OnTransactionCompleting(transactionTask, messages, tcs);
                }
            }
            catch (OperationCanceledException)
            {
                tcs.TrySetCanceled();
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }
        }


        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "The exception is set in the tcs")]
        private void OnTransactionCompleting(Task<bool> transactionTask, IList<Message> messages, TaskCompletionSource<object> tcs)
        {
            if (transactionTask.IsCompleted)
            {
                try
                {
                    OnTransactionComplete(transactionTask.Result, messages, tcs);
                }
                catch (OperationCanceledException)
                {
                    tcs.TrySetCanceled();
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            }
            else
            {
                transactionTask.Then(result => OnTransactionComplete(result, messages, tcs))
                               .ContinueWithNotComplete(tcs);
            }
        }

        private void OnTransactionComplete(bool success, IList<Message> messages, TaskCompletionSource<object> tcs)
        {
            if (success)
            {
                _trace.TraceVerbose("Transaction completed successfully");

                tcs.TrySetResult(null);
            }
            else
            {
                _trace.TraceVerbose("Transaction failed. Retrying...");

                _trace.TraceVerbose("Transaction disposed");
                
                SendImpl(messages, tcs);
            }
        }

        private Task<bool> ExecuteTransaction(long? newId, IList<Message> messages)
        {
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    _trace.TraceVerbose("ExecuteTransaction({0})", newId ?? 1);

                    byte[] data = MongoDbMessage.ToBytes(messages);


                    _database = _server.GetDatabase("SignalR");
                    MongoCollection<BsonDocument> collection = _database.GetCollection(_key);
                    collection.Insert(new BsonDocument()
                    {
                        {"id", newId ?? 1},
                        {"data", data}
                    });
                }
                catch (Exception ex)
                {
                    _trace.TraceError("Error in ExecuteTransaction: {0} - {1}", ex.Message, ex.StackTrace);
                    return false;
                }

                return true;
            });
        }

        private void OnMessage(ulong id, byte[] data)
        {
            // locked to avoid overlapping calls (even though we have set the mode 
            // to preserve order on the subscription)
            lock (_callbackLock)
            {
                // The key is the stream id (channel)
                var message = MongoDbMessage.FromBytes(id, data);

                OnReceived(0, message.Id, message.ScaleoutMessage);
            }
        }

        private void ConnectWithRetry()
        {
            Task connectTask = ConnectToMongoDb();

            connectTask.ContinueWith(task =>
            {
                if (task.IsFaulted)
                {
                    _trace.TraceError("Error connecting to MongoDb - " + task.Exception.GetBaseException());

                    if (_state == State.Disposing)
                    {
                        Shutdown();
                        return;
                    }

                    TaskAsyncHelper.Delay(ReconnectDelay)
                                   .Then(bus => bus.ConnectWithRetry(), this);
                }
                else
                {
                    var oldState = Interlocked.CompareExchange(ref _state,
                                                               State.Connected,
                                                               State.Closed);
                    if (oldState == State.Closed)
                    {
                        Open(0);
                    }
                    else if (oldState == State.Disposing)
                    {
                        Shutdown();
                    }
                }
            },
            TaskContinuationOptions.ExecuteSynchronously);
        }

        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Exceptions are caught")]
        private Task ConnectToMongoDb()
        {
            if (_connection != null)
            {
                _connection = null;
            }

            // Create a new connection to redis with the factory

            
            try
            {
                
                _trace.TraceInformation("Connecting...");
                MongoClient connection = _connectionFactory();
                _server = connection.GetServer();

                if (!_database.CollectionExists("LatestId"))
                {
                    _trace.TraceInformation("Created capped collection");
                    _database.CreateCollection("LatestId",
                                               new CollectionOptionsBuilder().SetCapped(true)
                                                                             .SetMaxDocuments(1));

                }

                if (!_database.CollectionExists(_key))
                {
                    _trace.TraceInformation("Created capped collection");
                    _database.CreateCollection(_key,
                                               new CollectionOptionsBuilder().SetCapped(true)
                                                                             .SetMaxSize(50*1024)
                                                                             .SetAutoIndexId(false));
                }

                return Task.Factory.StartNew(() =>
                {
                    BsonValue lastId = BsonMinKey.Value;
                    while (!_tailableCursorCancellation.IsCancellationRequested)
                    {
                        _database = _server.GetDatabase("SignalR");
                        MongoCollection<BsonDocument> collection = _database.GetCollection(_key);


                        var query = Query.GT("_id", lastId);
                        MongoCursor<BsonDocument> tailableCuror =
                            collection.Find(query).SetFlags(QueryFlags.TailableCursor | QueryFlags.AwaitData);

                        using (var enumerator = (MongoCursorEnumerator<BsonDocument>) tailableCuror.GetEnumerator())
                        {

                            while (true)
                            {
                                if (enumerator.MoveNext())
                                {
                                    
                                    var document = enumerator.Current;
                                    lastId = document["_id"];
                                    ulong id = 0;
                                    unchecked
                                    {
                                        //mongodb can not represent ulong, so store as long and then roll it over to a ulong
                                        id = (ulong)lastId.AsInt64;
                                    }
                                    OnMessage(id, document["data"].AsByteArray);
                                }
                                else
                                {
                                    if (enumerator.IsDead || _tailableCursorCancellation.IsCancellationRequested)
                                    {
                                        break;
                                    }
                                    if (!enumerator.IsServerAwaitCapable)
                                    {
                                        Thread.Sleep(TimeSpan.FromMilliseconds(100));
                                    }
                                }

                                if(_tailableCursorCancellation.IsCancellationRequested)
                                {
                                    break;
                                }
                            }
                        }
                    }

                }, _tailableCursorCancellation.Token);
            }
            catch (Exception ex)
            {
                _trace.TraceError("Error connecting to MongoDb - " + ex.GetBaseException());

                return TaskAsyncHelper.FromError(ex);
            }
        }

        private static class State
        {
            public const int Closed = 0;
            public const int Connected = 1;
            public const int Disposing = 2;
            public const int Disposed = 3;
        }
    }
}
