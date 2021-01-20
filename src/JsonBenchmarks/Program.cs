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