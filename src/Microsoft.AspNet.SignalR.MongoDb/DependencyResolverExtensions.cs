// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using BookSleeve;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNet.SignalR.MongoDb;

namespace Microsoft.AspNet.SignalR
{
    public static class DependencyResolverExtensions
    {
        /// <summary>
        /// Use Redis as the messaging backplane for scaling out of ASP.NET SignalR applications in a web farm.
        /// </summary>
        /// <param name="resolver">The dependency resolver.</param>
        /// <param name="connectionString">The MongoDB connection string.</param>
        /// <param name="eventKey">The MongoDB event key to use.</param>
        /// <returns>The dependency resolver.</returns>
        public static IDependencyResolver UseMongoDb(this IDependencyResolver resolver, string connectionString, string eventKey)
        {
            var configuration = new MongoDbScaleoutConfiguration(connectionString, eventKey);

            return UseMongoDb(resolver, configuration);
        }

        /// <summary>
        /// Use Redis as the messaging backplane for scaling out of ASP.NET SignalR applications in a web farm.
        /// </summary>
        /// <param name="resolver">The dependency resolver</param>
        /// <param name="configuration">The Redis scale-out configuration options.</param> 
        /// <returns>The dependency resolver.</returns>
        public static IDependencyResolver UseMongoDb(this IDependencyResolver resolver, MongoDbScaleoutConfiguration configuration)
        {
            var bus = new Lazy<MongoDbMessageBus>(() => new MongoDbMessageBus(resolver, configuration));
            resolver.Register(typeof(IMessageBus), () => bus.Value);

            return resolver;
        }
    }
}
