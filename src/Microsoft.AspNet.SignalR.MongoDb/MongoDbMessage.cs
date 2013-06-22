// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.AspNet.SignalR.Messaging;

namespace Microsoft.AspNet.SignalR.MongoDb
{
    public class MongoDbMessage
    {
        public ulong Id { get; private set; }
        public ScaleoutMessage ScaleoutMessage { get; private set; }

        public static byte[] ToBytes(IList<Message> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }

            var scaleoutMessage = new ScaleoutMessage(messages);
            return scaleoutMessage.ToBytes();
        }

        public static MongoDbMessage FromBytes(ulong id, byte[] data)
        {
            var message = new MongoDbMessage
            {
                Id = id,
                ScaleoutMessage = ScaleoutMessage.FromBytes(data)
            };
            return message;
        }
    }
}
