// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using BookSleeve;
using Microsoft.AspNet.SignalR.Messaging;
using MongoDB.Driver;

namespace Microsoft.AspNet.SignalR
{
    /// <summary>
    /// Settings for the Redis scale-out message bus implementation.
    /// </summary>
    public class MongoDbScaleoutConfiguration : ScaleoutConfiguration
    {
        public MongoDbScaleoutConfiguration(string connectionString, string eventKey)
            : this(MakeConnectionFactory(connectionString), eventKey)
        {

        }

        public MongoDbScaleoutConfiguration(Func<MongoClient> connectionFactory, string eventKey)
        {
            if (connectionFactory == null)
            {
                throw new ArgumentNullException("connectionFactory");
            }

            if (eventKey == null)
            {
                throw new ArgumentNullException("eventKey");
            }

            ConnectionFactory = connectionFactory;
            EventKey = eventKey;
        }

        internal Func<MongoClient> ConnectionFactory { get; private set; }

        /// <summary>
        /// The Redis event key to use.
        /// </summary>
        public string EventKey { get; private set; }

        private static Func<MongoClient> MakeConnectionFactory(string connectionString)
        {
            return () => new MongoClient(connectionString);
        }
    }
}
