using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using JsonBenchmarks;
using Microsoft.Diagnostics.Tracing.Parsers;
using Newtonsoft.Json;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

BenchmarkRunner.Run<Benchmarks>();

//var thing = new Benchmarks();
//thing.Setup();

//for (int i = 0; i < 100; i++)
//{
//    thing.CustomReaderBenchmark();
//}

//Console.WriteLine("NEXT");
//Console.ReadKey();

//thing.CustomReaderBenchmark();

//Console.WriteLine("NEXT");
//Console.ReadKey();

namespace JsonBenchmarks
{
    [MemoryDiagnoser]
    public class Benchmarks
    {
        private const string JsonResponse = @"{
          ""cluster_name"" : ""testcluster"",
          ""status"" : ""yellow"",
          ""timed_out"" : false,
          ""number_of_nodes"" : 1,
          ""number_of_data_nodes"" : 2,
          ""active_primary_shards"" : 3,
          ""active_shards"" : 4,
          ""relocating_shards"" : 5,
          ""initializing_shards"" : 6,
          ""unassigned_shards"" : 7,
          ""delayed_unassigned_shards"": 8,
          ""number_of_pending_tasks"" : 9,
          ""number_of_in_flight_fetch"": 10,
          ""task_max_waiting_in_queue_millis"": 100,
          ""active_shards_percent_as_number"": 50.0
        }";

        private Stream _stream;

        public ClusterHealthResponse ClusterHealthResponse { get; private set; }

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
        }
        };

        [GlobalSetup]
        public void Setup()
        {
            _stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonResponse));
        }

        //[Benchmark]
        //public void NewtonsoftJsonBenchmark()
        //{
        //    _stream.Position = 0;

        //    using var sr = new StreamReader(_stream, leaveOpen: true);
        //    using var reader = new JsonTextReader(sr);
        //    var serializer = new JsonSerializer();

        //    ClusterHealthResponse = serializer.Deserialize<ClusterHealthResponse>(reader);
        //}

        //[Benchmark]
        //public async Task Utf8JsonBenchmark()
        //{
        //    _stream.Position = 0;
        //    ClusterHealthResponse = await Utf8Json.JsonSerializer.DeserializeAsync<ClusterHealthResponse>(_stream);
        //}

        //[Benchmark]
        //public async Task SystemTextJsonBenchmark()
        //{
        //    _stream.Position = 0;
        //    ClusterHealthResponse = await System.Text.Json.JsonSerializer.DeserializeAsync<ClusterHealthResponse>(_stream, JsonOptions);
        //}

        [Benchmark]
        public void CustomReaderBenchmark()
        {
            _stream.Position = 0;
            var reader = new ClusterHealthResponseReader(_stream);
            ClusterHealthResponse = reader.Read();
        }

        [Benchmark]
        public void CustomReaderBenchmarkTwo()
        {
            _stream.Position = 0;
            var reader = new ClusterHealthResponseReaderTwo(_stream);
            ClusterHealthResponse = reader.Read();
        }
    }

    public struct ClusterHealthResponseReaderTwo
    {
        private static ReadOnlySpan<byte> ClusterName => new[] { (byte)'c', (byte)'l', (byte)'u', (byte)'s', (byte)'t', (byte)'e', (byte)'r', (byte)'_', (byte)'n', (byte)'a', (byte)'m', (byte)'e' };
        private static ReadOnlySpan<byte> ActivePrimaryShards => new[] { (byte)'a', (byte)'c', (byte)'t', (byte)'i', (byte)'v', (byte)'e', (byte)'_', (byte)'p', (byte)'r', (byte)'i', (byte)'m', (byte)'a', (byte)'r', (byte)'y', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s' };
        private static ReadOnlySpan<byte> ActiveShards => new[] { (byte)'a', (byte)'c', (byte)'t', (byte)'i', (byte)'v', (byte)'e', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s' };
        private static ReadOnlySpan<byte> ActiveShardsPercentAsNumber => new[] { (byte)'a', (byte)'c', (byte)'t', (byte)'i', (byte)'v', (byte)'e', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s', (byte)'_', (byte)'p', (byte)'e', (byte)'r', (byte)'c', (byte)'e', (byte)'n', (byte)'t', (byte)'_', (byte)'a', (byte)'s', (byte)'_', (byte)'n', (byte)'u', (byte)'m', (byte)'b', (byte)'e', (byte)'r' };
        private static ReadOnlySpan<byte> DelayedUnassignedShards => new[] { (byte)'d', (byte)'e', (byte)'l', (byte)'a', (byte)'y', (byte)'e', (byte)'d', (byte)'_', (byte)'u', (byte)'n', (byte)'a', (byte)'s', (byte)'s', (byte)'i', (byte)'g', (byte)'n', (byte)'e', (byte)'d', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s' };
        private static ReadOnlySpan<byte> InitializingShards => new[] { (byte)'i', (byte)'n', (byte)'i', (byte)'t', (byte)'i', (byte)'a', (byte)'l', (byte)'i', (byte)'z', (byte)'i', (byte)'n', (byte)'g', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s' };
        private static ReadOnlySpan<byte> NumberOfDataNodes => new[] { (byte)'n', (byte)'u', (byte)'m', (byte)'b', (byte)'e', (byte)'r', (byte)'_', (byte)'o', (byte)'f', (byte)'_', (byte)'d', (byte)'a', (byte)'t', (byte)'a', (byte)'_', (byte)'n', (byte)'o', (byte)'d', (byte)'e', (byte)'s' };
        private static ReadOnlySpan<byte> NumberOfInFlightFetch => new[] { (byte)'n', (byte)'u', (byte)'m', (byte)'b', (byte)'e', (byte)'r', (byte)'_', (byte)'o', (byte)'f', (byte)'_', (byte)'i', (byte)'n', (byte)'_', (byte)'f', (byte)'l', (byte)'i', (byte)'g', (byte)'h', (byte)'t', (byte)'_', (byte)'f', (byte)'e', (byte)'t', (byte)'c', (byte)'h' };
        private static ReadOnlySpan<byte> NumberOfNodes => new[] { (byte)'n', (byte)'u', (byte)'m', (byte)'b', (byte)'e', (byte)'r', (byte)'_', (byte)'o', (byte)'f', (byte)'_', (byte)'n', (byte)'o', (byte)'d', (byte)'e', (byte)'s' };
        private static ReadOnlySpan<byte> NumberOfPendingTasks => new[] { (byte)'n', (byte)'u', (byte)'m', (byte)'b', (byte)'e', (byte)'r', (byte)'_', (byte)'o', (byte)'f', (byte)'_', (byte)'p', (byte)'e', (byte)'n', (byte)'d', (byte)'i', (byte)'n', (byte)'g', (byte)'_', (byte)'t', (byte)'a', (byte)'s', (byte)'k', (byte)'s' };
        private static ReadOnlySpan<byte> RelocatingShards => new[] { (byte)'r', (byte)'e', (byte)'l', (byte)'o', (byte)'c', (byte)'a', (byte)'t', (byte)'i', (byte)'n', (byte)'g', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s' };
        private static ReadOnlySpan<byte> Status => new[] { (byte)'s', (byte)'t', (byte)'a', (byte)'t', (byte)'u', (byte)'s' };
        private static ReadOnlySpan<byte> TaskMaxWaitingInQueueMillis => new[] { (byte)'t', (byte)'a', (byte)'s', (byte)'k', (byte)'_', (byte)'m', (byte)'a', (byte)'x', (byte)'_', (byte)'w', (byte)'a', (byte)'i', (byte)'t', (byte)'i', (byte)'n', (byte)'g', (byte)'_', (byte)'i', (byte)'n', (byte)'_', (byte)'q', (byte)'u', (byte)'e', (byte)'u', (byte)'e', (byte)'_', (byte)'m', (byte)'i', (byte)'l', (byte)'l', (byte)'i', (byte)'s' };
        private static ReadOnlySpan<byte> TimedOut => new[] { (byte)'t', (byte)'i', (byte)'m', (byte)'e', (byte)'d', (byte)'_', (byte)'o', (byte)'u', (byte)'t' };
        private static ReadOnlySpan<byte> UnassignedShards => new[] { (byte)'u', (byte)'n', (byte)'a', (byte)'s', (byte)'s', (byte)'i', (byte)'g', (byte)'n', (byte)'e', (byte)'d', (byte)'_', (byte)'s', (byte)'h', (byte)'a', (byte)'r', (byte)'d', (byte)'s' };
        
        private static ReadOnlySpan<byte> YellowSpan => new[] { (byte)'y', (byte)'e', (byte)'l', (byte)'l', (byte)'o', (byte)'w' };
        private static ReadOnlySpan<byte> GreenSpan => new[] { (byte)'g', (byte)'r', (byte)'e', (byte)'e', (byte)'n' };
        private static ReadOnlySpan<byte> RedSpan => new[] { (byte)'r', (byte)'e', (byte)'d' };

        private readonly Stream _stream;
        private int _tokenCounter;
        private int _propertyCounter;
        private bool _done;
        private readonly JsonReaderState _jsonReaderState;

        public ClusterHealthResponseReaderTwo(Stream stream)
        {
            _stream = stream;
            _tokenCounter = 0;
            _propertyCounter = -1;
            _done = false;
            _jsonReaderState = new JsonReaderState(default);
        }

        public ClusterHealthResponse Read()
        {
            ClusterHealthResponse response = null;

            var buffer = ArrayPool<byte>.Shared.Rent(4096);

            try
            {
                var totalBytesRead = 0;
                int lastReadBytes;

                while ((lastReadBytes = _stream.Read(buffer, 0, buffer.Length)) > 0 && !_done)
                {
                    totalBytesRead += lastReadBytes;

                    if (lastReadBytes > 0 && response is null)
                        response = new ClusterHealthResponse();

                    Process(buffer.AsSpan().Slice(0, lastReadBytes), response, totalBytesRead == _stream.Length);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return response;
        }

        private void Process(ReadOnlySpan<byte> data, ClusterHealthResponse response, bool isLastBlock)
        {
            var reader = new Utf8JsonReader(data, isLastBlock, _jsonReaderState);

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    _done = true;
                    break;
                }

                if (_tokenCounter == 0)
                {
                    if (reader.TokenType != JsonTokenType.StartObject)
                        throw new System.Text.Json.JsonException();
                }
                else if (_tokenCounter % 2 != 0)
                {
                    // TODO: Needs to be robust for sequences!!
                    var workingSpan = reader.HasValueSequence
                        ? reader.ValueSequence.FirstSpan
                        : reader.ValueSpan;
                    
                    _propertyCounter = -1;

                    if (workingSpan.SequenceEqual(ActivePrimaryShards))
                        _propertyCounter = 0;
                    else if (workingSpan.SequenceEqual(ActiveShards))
                        _propertyCounter = 1;
                    else if (workingSpan.SequenceEqual(ActiveShardsPercentAsNumber))
                        _propertyCounter = 2;
                    else if (workingSpan.SequenceEqual(ClusterName))
                        _propertyCounter = 3;
                    else if (workingSpan.SequenceEqual(DelayedUnassignedShards))
                        _propertyCounter = 4;
                    else if (workingSpan.SequenceEqual(InitializingShards))
                        _propertyCounter = 5;
                    else if (workingSpan.SequenceEqual(NumberOfDataNodes))
                        _propertyCounter = 6;
                    else if (workingSpan.SequenceEqual(NumberOfInFlightFetch))
                        _propertyCounter = 7;
                    else if (workingSpan.SequenceEqual(NumberOfNodes))
                        _propertyCounter = 8;
                    else if (workingSpan.SequenceEqual(NumberOfPendingTasks))
                        _propertyCounter = 9;
                    else if (workingSpan.SequenceEqual(RelocatingShards))
                        _propertyCounter = 10;
                    else if (workingSpan.SequenceEqual(Status))
                        _propertyCounter = 11;
                    else if (workingSpan.SequenceEqual(TaskMaxWaitingInQueueMillis))
                        _propertyCounter = 12;
                    else if (workingSpan.SequenceEqual(TimedOut))
                        _propertyCounter = 13;
                    else if (workingSpan.SequenceEqual(UnassignedShards))
                        _propertyCounter = 14;
                }
                else
                {
                    switch (_propertyCounter)
                    {
                        case 0:
                            response.ActivePrimaryShards = reader.GetInt32();
                            break;
                        case 1:
                            response.ActiveShards = reader.GetInt32();
                            break;
                        case 2:
                            response.ActiveShardsPercentAsNumber = reader.GetDouble();
                            break;
                        case 3:
                            //if (reader.TokenType != JsonTokenType.String)
                            //    throw new System.Text.Json.JsonException();
                            response.ClusterName = reader.GetString();
                            break;
                        case 4:
                            response.DelayedUnassignedShards = reader.GetInt32();
                            break;
                        case 5:
                            response.InitializingShards = reader.GetInt32();
                            break;
                        case 6:
                            response.NumberOfDataNodes = reader.GetInt32();
                            break;
                        case 7:
                            response.NumberOfInFlightFetch = reader.GetInt32();
                            break;
                        case 8:
                            response.NumberOfNodes = reader.GetInt32();
                            break;
                        case 9:
                            response.NumberOfPendingTasks = reader.GetInt32();
                            break;
                        case 10:
                            response.RelocatingShards = reader.GetInt32();
                            break;
                        case 11:
                            // This is not necessarily valid
                            var workingSpan = reader.HasValueSequence
                                ? reader.ValueSequence.FirstSpan
                                : reader.ValueSpan;
                            if (workingSpan.SequenceEqual(YellowSpan))
                                response.Status = Health.Yellow;
                            else if (workingSpan.SequenceEqual(RedSpan))
                                response.Status = Health.Red;
                            else if (workingSpan.SequenceEqual(GreenSpan))
                                response.Status = Health.Green;
                            break;
                        case 12:
                            response.TaskMaxWaitTimeInQueueInMilliseconds = reader.GetInt64();
                            break;
                        case 13:
                            response.TimedOut = reader.GetBoolean();
                            break;
                        case 14:
                            response.UnassignedShards = reader.GetInt32();
                            break;
                    }
                }

                _tokenCounter++;
            }
        }

        private static void AssertNumber(JsonTokenType tokenType)
        {
            //if (tokenType != JsonTokenType.Number)
            //    throw new System.Text.Json.JsonException();
        }
    }

    public struct ClusterHealthResponseReader
    {
        private static ReadOnlySpan<byte> YellowSpan => new[] { (byte)'y', (byte)'e', (byte)'l', (byte)'l', (byte)'o', (byte)'w' };
        private static ReadOnlySpan<byte> GreenSpan => new[] { (byte)'g', (byte)'r', (byte)'e', (byte)'e', (byte)'n' };
        private static ReadOnlySpan<byte> RedSpan => new[] { (byte)'r', (byte)'e', (byte)'d' };

        private readonly Stream _stream;
        private int _tokenCounter;
        private readonly JsonReaderState _jsonReaderState;

        public ClusterHealthResponseReader(Stream stream)
        {
            _stream = stream;
            _tokenCounter = 0;
            _jsonReaderState = new JsonReaderState(default);
        }

        public ClusterHealthResponse Read()
        {
            ClusterHealthResponse response = null;
            
            var buffer = ArrayPool<byte>.Shared.Rent(4096);

            try
            {
                var totalBytesRead = 0;
                int lastReadBytes;

                while ((lastReadBytes = _stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    totalBytesRead += lastReadBytes;

                    if (lastReadBytes > 0 && response is null)
                        response = new ClusterHealthResponse();
                    
                    Process(buffer.AsSpan().Slice(0, lastReadBytes), response, totalBytesRead == _stream.Length);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return response;
        }

        private void Process(ReadOnlySpan<byte> data, ClusterHealthResponse response, bool isLastBlock)
        {
            var reader = new Utf8JsonReader(data, isLastBlock, _jsonReaderState);

            while (reader.Read())
            {
                if (_tokenCounter == 0)
                {
                    //if (reader.TokenType != JsonTokenType.StartObject)
                    //    throw new System.Text.Json.JsonException();
                }
                else if (_tokenCounter == 31)
                {
                    //if (reader.TokenType != JsonTokenType.EndObject)
                    //    throw new System.Text.Json.JsonException();
                }
                else if (_tokenCounter % 2 != 0)
                {
                    //if (reader.TokenType != JsonTokenType.PropertyName)
                    //    throw new System.Text.Json.JsonException();
                }
                else
                {
                    switch (_tokenCounter)
                    {
                        case 2:
                            //if (reader.TokenType != JsonTokenType.String)
                            //    throw new System.Text.Json.JsonException();
                            response.ClusterName = reader.GetString();
                            break;
                        case 4:
                            // This is not necessarily valid
                            var workingSpan = reader.HasValueSequence
                                ? reader.ValueSequence.FirstSpan
                                : reader.ValueSpan;

                            if (workingSpan.SequenceEqual(YellowSpan))
                                response.Status = Health.Yellow;
                            else if (workingSpan.SequenceEqual(RedSpan))
                                response.Status = Health.Red;
                            else if (workingSpan.SequenceEqual(GreenSpan))
                                response.Status = Health.Green;
                            
                            break;
                        case 6:
                            //if (reader.TokenType != JsonTokenType.False && reader.TokenType != JsonTokenType.True)
                            //    throw new System.Text.Json.JsonException();
                            response.TimedOut = reader.GetBoolean();
                            break;
                        case 8:
                            //AssertNumber(reader.TokenType);
                            response.NumberOfNodes = reader.GetInt32();
                            break;
                        case 10:
                            //AssertNumber(reader.TokenType);
                            response.NumberOfDataNodes = reader.GetInt32();
                            break;
                        case 12:
                            //AssertNumber(reader.TokenType);
                            response.ActivePrimaryShards = reader.GetInt32();
                            break;
                        case 14:
                            //AssertNumber(reader.TokenType);
                            response.ActiveShards = reader.GetInt32();
                            break;
                        case 16:
                            //AssertNumber(reader.TokenType);
                            response.RelocatingShards = reader.GetInt32();
                            break;
                        case 18:
                            //AssertNumber(reader.TokenType);
                            response.InitializingShards = reader.GetInt32();
                            break;
                        case 20:
                            //AssertNumber(reader.TokenType);
                            response.UnassignedShards = reader.GetInt32();
                            break;
                        case 22:
                            //AssertNumber(reader.TokenType);
                            response.DelayedUnassignedShards = reader.GetInt32();
                            break;
                        case 24:
                            //AssertNumber(reader.TokenType);
                            response.NumberOfPendingTasks = reader.GetInt32();
                            break;
                        case 26:
                            //AssertNumber(reader.TokenType);
                            response.NumberOfInFlightFetch = reader.GetInt32();
                            break;
                        case 28:
                            //AssertNumber(reader.TokenType);
                            response.TaskMaxWaitTimeInQueueInMilliseconds = reader.GetInt64();
                            break;
                        case 30:
                            //AssertNumber(reader.TokenType);
                            response.ActiveShardsPercentAsNumber = reader.GetDouble();
                            break;
                    }
                }

                _tokenCounter++;
            }
        }

        private static void AssertNumber(JsonTokenType tokenType)
        {
            //if (tokenType != JsonTokenType.Number)
            //    throw new System.Text.Json.JsonException();
        }
    }

    [DataContract]
    public class ClusterHealthResponse
    {
        [JsonPropertyName("active_primary_shards")]
        [DataMember(Name = "active_primary_shards")]
        public int ActivePrimaryShards { get; set; }

        [JsonPropertyName("active_shards")]
        [DataMember(Name = "active_shards")]
        public int ActiveShards { get; set; }

        [JsonPropertyName("active_shards_percent_as_number")]
        [DataMember(Name = "active_shards_percent_as_number")]
        public double ActiveShardsPercentAsNumber { get; set; }

        [JsonPropertyName("cluster_name")]
        [DataMember(Name = "cluster_name")]
        public string ClusterName { get; set; }

        [JsonPropertyName("delayed_unassigned_shards")]
        [DataMember(Name = "delayed_unassigned_shards")]
        public int DelayedUnassignedShards { get; set; }

        [JsonPropertyName("initializing_shards")]
        [DataMember(Name = "initializing_shards")]
        public int InitializingShards { get; set; }

        [JsonPropertyName("number_of_data_nodes")]
        [DataMember(Name = "number_of_data_nodes")]
        public int NumberOfDataNodes { get; set; }

        [JsonPropertyName("number_of_in_flight_fetch")]
        [DataMember(Name = "number_of_in_flight_fetch")]
        public int NumberOfInFlightFetch { get; set; }

        [JsonPropertyName("number_of_nodes")]
        [DataMember(Name = "number_of_nodes")]
        public int NumberOfNodes { get; set; }

        [JsonPropertyName("number_of_pending_tasks")]
        [DataMember(Name = "number_of_pending_tasks")]
        public int NumberOfPendingTasks { get; set; }

        [JsonPropertyName("relocating_shards")]
        [DataMember(Name = "relocating_shards")]
        public int RelocatingShards { get; set; }

        [JsonPropertyName("status")]
        [DataMember(Name = "status")]
        public Health Status { get; set; }

        [JsonPropertyName("task_max_waiting_in_queue_millis")]
        [DataMember(Name = "task_max_waiting_in_queue_millis")]
        public long TaskMaxWaitTimeInQueueInMilliseconds { get; set; }

        [JsonPropertyName("timed_out")]
        [DataMember(Name = "timed_out")]
        public bool TimedOut { get; set; }

        [JsonPropertyName("unassigned_shards")]
        [DataMember(Name = "unassigned_shards")]
        public int UnassignedShards { get; set; }
    }

    public enum Health
    {
        [EnumMember(Value = "green")]
        Green,
        [EnumMember(Value = "yellow")]
        Yellow,
        [EnumMember(Value = "red")]
        Red
    }
}