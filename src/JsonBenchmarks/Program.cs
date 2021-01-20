using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using JsonBenchmarks;
using Newtonsoft.Json;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

BenchmarkRunner.Run<Benchmarks>();

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

        [Benchmark]
        public void NewtonsoftJsonBenchmark()
        {
            _stream.Position = 0;

            using var sr = new StreamReader(_stream, leaveOpen: true);
            using var reader = new JsonTextReader(sr);
            var serializer = new JsonSerializer();

            ClusterHealthResponse = serializer.Deserialize<ClusterHealthResponse>(reader);
        }

        [Benchmark]
        public async Task Utf8JsonBenchmark()
        {
            _stream.Position = 0;
            ClusterHealthResponse = await Utf8Json.JsonSerializer.DeserializeAsync<ClusterHealthResponse>(_stream);
        }

        [Benchmark]
        public async Task SystemTextJsonBenchmark()
        {
            _stream.Position = 0;
            ClusterHealthResponse = await System.Text.Json.JsonSerializer.DeserializeAsync<ClusterHealthResponse>(_stream, JsonOptions);
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