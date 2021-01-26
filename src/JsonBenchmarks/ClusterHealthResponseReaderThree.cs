using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Text.Json;
using static System.Text.Encoding;

namespace JsonBenchmarks
{
    public struct ClusterHealthResponseReaderThree
    {
        private static readonly ushort _properties = 0;

        // Not sure if this still benefits from: https://vcsjones.dev/csharp-readonly-span-bytes-static/
        // Seems to be allocating exactly the same
        
        private static readonly ushort _clusterNameProp = ++_properties;
        private static readonly byte[] _clusterName = UTF8.GetBytes("cluster_name");
        private static ReadOnlySpan<byte> ClusterName => _clusterName;

        private static readonly ushort _activePrimaryShardsProp = ++_properties;
        private static readonly byte[] _activePrimaryShards = UTF8.GetBytes("active_primary_shards");
        private static ReadOnlySpan<byte> ActivePrimaryShards => _activePrimaryShards;

        private static readonly ushort _activeShardsProp = ++_properties;
        private static readonly byte[] _activeShards = UTF8.GetBytes("active_shards");
        private static ReadOnlySpan<byte> ActiveShards => _activeShards;

        private static readonly ushort _activeShardsPercentAsNumberProp = ++_properties;
        private static readonly byte[] _activeShardsPercentAsNumber = UTF8.GetBytes("active_shards_percent_as_number");
        private static ReadOnlySpan<byte> ActiveShardsPercentAsNumber => _activeShardsPercentAsNumber;

        private static readonly ushort _delayedUnassignedShardsProp = ++_properties;
        private static readonly byte[] _delayedUnassignedShards = UTF8.GetBytes("delayed_unassigned_shards");
        private static ReadOnlySpan<byte> DelayedUnassignedShards => _delayedUnassignedShards;

        private static readonly ushort _initializingShardsProp = ++_properties;
        private static readonly byte[] _initializingShards = UTF8.GetBytes("initializing_shards");
        private static ReadOnlySpan<byte> InitializingShards => _initializingShards;

        private static readonly ushort _numberOfDataNodesProp = ++_properties;
        private static readonly byte[] _numberOfDataNodes = UTF8.GetBytes("number_of_data_nodes");
        private static ReadOnlySpan<byte> NumberOfDataNodes => _numberOfDataNodes;

        private static readonly ushort _numberOfInFlightFetchProp = ++_properties;
        private static readonly byte[] _numberOfInFlightFetch = UTF8.GetBytes("number_of_in_flight_fetch");
        private static ReadOnlySpan<byte> NumberOfInFlightFetch => _numberOfInFlightFetch;

        private static readonly ushort _numberOfNodesProp = ++_properties;
        private static readonly byte[] _numberOfNodes = UTF8.GetBytes("number_of_nodes");
        private static ReadOnlySpan<byte> NumberOfNodes => _numberOfNodes;

        private static readonly ushort _numberOfPendingTasksProp = ++_properties;
        private static readonly byte[] _numberOfPendingTasks = UTF8.GetBytes("number_of_pending_tasks");
        private static ReadOnlySpan<byte> NumberOfPendingTasks => _numberOfPendingTasks;

        private static readonly ushort _relocatingShardsProp = ++_properties;
        private static readonly byte[] _relocatingShards = UTF8.GetBytes("relocating_shards");
        private static ReadOnlySpan<byte> RelocatingShards => _relocatingShards;

        private static readonly ushort _statusProp = ++_properties;
        private static readonly byte[] _status = UTF8.GetBytes("status");
        private static ReadOnlySpan<byte> Status => _status;

        private static readonly ushort _taskMaxWaitingInQueueMillisProp = ++_properties;
        private static readonly byte[] _taskMaxWaitingInQueueMillis = UTF8.GetBytes("task_max_waiting_in_queue_millis");
        private static ReadOnlySpan<byte> TaskMaxWaitingInQueueMillis => _taskMaxWaitingInQueueMillis;

        private static readonly ushort _timedOutProp = ++_properties;
        private static readonly byte[] _timedOut = UTF8.GetBytes("timed_out");
        private static ReadOnlySpan<byte> TimedOut => _timedOut;

        private static readonly ushort _unassignedShardsProp = ++_properties;
        private static readonly byte[] _unassignedShards = UTF8.GetBytes("unassigned_shards");
        private static ReadOnlySpan<byte> UnassignedShards => _unassignedShards;
        
        //values
        private static readonly byte[] _yellowSpan = UTF8.GetBytes("yellow");
        private static ReadOnlySpan<byte> YellowSpan => _yellowSpan;

        private static readonly byte[] _greenSpan = UTF8.GetBytes("green");
        private static ReadOnlySpan<byte> GreenSpan => _greenSpan;

        private static readonly byte[] _redSpan = UTF8.GetBytes("red");
        private static ReadOnlySpan<byte> RedSpan => _redSpan;

        private readonly Stream _stream;
        private ushort _propertyRef;
        private readonly JsonReaderState _jsonReaderState;

        public ClusterHealthResponseReaderThree(Stream stream)
        {
            _stream = stream;
            _propertyRef = 0;
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
                if (reader.TokenType == JsonTokenType.PropertyName && reader.CurrentDepth == 1)
                {

                    // TODO: Needs to be robust for sequences!!
                    var workingSpan = reader.HasValueSequence
                        ? reader.ValueSequence.FirstSpan
                        : reader.ValueSpan;

                    if (workingSpan.SequenceEqual(ActivePrimaryShards))
                        _propertyRef = _activePrimaryShardsProp;
                    else if (workingSpan.SequenceEqual(ActiveShards))
                        _propertyRef = _activeShardsProp;
                    else if (workingSpan.SequenceEqual(ActiveShardsPercentAsNumber))
                        _propertyRef = _activeShardsPercentAsNumberProp;
                    else if (workingSpan.SequenceEqual(ClusterName))
                        _propertyRef = _clusterNameProp;
                    else if (workingSpan.SequenceEqual(DelayedUnassignedShards))
                        _propertyRef = _delayedUnassignedShardsProp;
                    else if (workingSpan.SequenceEqual(InitializingShards))
                        _propertyRef = _initializingShardsProp;
                    else if (workingSpan.SequenceEqual(NumberOfDataNodes))
                        _propertyRef = _numberOfDataNodesProp;
                    else if (workingSpan.SequenceEqual(NumberOfInFlightFetch))
                        _propertyRef = _numberOfInFlightFetchProp;
                    else if (workingSpan.SequenceEqual(NumberOfNodes))
                        _propertyRef = _numberOfNodesProp;
                    else if (workingSpan.SequenceEqual(NumberOfPendingTasks))
                        _propertyRef = _numberOfPendingTasksProp;
                    else if (workingSpan.SequenceEqual(RelocatingShards))
                        _propertyRef = _relocatingShardsProp;
                    else if (workingSpan.SequenceEqual(Status))
                        _propertyRef = _statusProp;
                    else if (workingSpan.SequenceEqual(TaskMaxWaitingInQueueMillis))
                        _propertyRef = _taskMaxWaitingInQueueMillisProp;
                    else if (workingSpan.SequenceEqual(TimedOut))
                        _propertyRef = _timedOutProp;
                    else if (workingSpan.SequenceEqual(UnassignedShards))
                        _propertyRef = _unassignedShardsProp;
                }
                else if (_propertyRef != 0)
                {
                    if (_propertyRef == _activePrimaryShardsProp)
                        response.ActivePrimaryShards = reader.GetInt32();
                    else if (_propertyRef == _activeShardsProp)
                        response.ActiveShards = reader.GetInt32();
                    else if (_propertyRef == _activeShardsPercentAsNumberProp)
                        response.ActiveShardsPercentAsNumber = reader.GetDouble();
                    else if (_propertyRef == _clusterNameProp)
                        //if (reader.TokenType != JsonTokenType.String)
                        //    throw new System.Text.Json.JsonException();
                        response.ClusterName = reader.GetString();
                    else if (_propertyRef == _delayedUnassignedShardsProp)
                        response.DelayedUnassignedShards = reader.GetInt32();
                    else if (_propertyRef == _initializingShardsProp)
                        response.InitializingShards = reader.GetInt32();
                    else if (_propertyRef == _numberOfDataNodesProp)
                        response.NumberOfDataNodes = reader.GetInt32();
                    else if (_propertyRef == _numberOfInFlightFetchProp)
                        response.NumberOfInFlightFetch = reader.GetInt32();
                    else if (_propertyRef == _numberOfNodesProp)
                        response.NumberOfNodes = reader.GetInt32();
                    else if (_propertyRef == _numberOfPendingTasksProp)
                        response.NumberOfPendingTasks = reader.GetInt32();
                    else if (_propertyRef == _relocatingShardsProp)
                        response.RelocatingShards = reader.GetInt32();
                    else if (_propertyRef == _statusProp)
                    {
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
                    }
                    else if (_propertyRef == _taskMaxWaitingInQueueMillisProp)
                        response.TaskMaxWaitTimeInQueueInMilliseconds = reader.GetInt64();
                    else if (_propertyRef == _timedOutProp)
                        response.TimedOut = reader.GetBoolean();
                    else if (_propertyRef == _unassignedShardsProp)
                        response.UnassignedShards = reader.GetInt32();
                    _propertyRef = 0;
                }
            }
        }
    }
}