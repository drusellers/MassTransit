﻿// Copyright 2007-2016 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.
namespace MassTransit.HttpTransport
{
    using System.IO;
    using System.Net.Mime;
    using Serialization;


    public class HttpSerializationWrapper : IMessageSerializer
    {
        readonly JsonMessageSerializer _json;

        public HttpSerializationWrapper()
        {
            _json = new JsonMessageSerializer();
        }

        public ContentType ContentType => _json.ContentType;

        public void Serialize<T>(Stream stream, SendContext<T> context) where T : class
        {
            _json.Serialize(stream, context);
        }
    }
}