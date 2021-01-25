using System.Threading.Tasks;
using FluentAssertions;
using JsonBenchmarks;
using Xunit;

namespace JsonBenchmarksTests
{
    public class BenchmarkTests
    {
        //[Fact]
        //public void NewtonsoftJsonTest()
        //{
        //    var sut = new Benchmarks();
        //    sut.Setup();
        //    sut.NewtonsoftJsonBenchmark();

        //    AssertResponse(sut.ClusterHealthResponse);
        //}

        //[Fact]
        //public async Task Utf8JsonTest()
        //{
        //    var sut = new Benchmarks();
        //    sut.Setup();
        //    await sut.Utf8JsonBenchmark();

        //    AssertResponse(sut.ClusterHealthResponse);
        //}

        //[Fact]
        //public async Task SystemTextJsonTest()
        //{
        //    var sut = new Benchmarks();
        //    sut.Setup();
        //    await sut.SystemTextJsonBenchmark();

        //    AssertResponse(sut.ClusterHealthResponse);
        //}

        [Fact]
        public void CustomReaderTest()
        {
            var sut = new Benchmarks();
            sut.Setup();
            sut.CustomReaderBenchmark();

            AssertResponse(sut.ClusterHealthResponse);
        }

        [Fact]
        public void CustomReaderTwoTest()
        {
            var sut = new Benchmarks();
            sut.Setup();
            sut.CustomReaderBenchmarkTwo();

            AssertResponse(sut.ClusterHealthResponse);
        }

        private static void AssertResponse(ClusterHealthResponse response)
        {
            response.Should().NotBeNull();
            response.ClusterName.Should().Be("testcluster");
            response.Status.Should().Be(Health.Yellow);
            response.TimedOut.Should().BeFalse();
            response.NumberOfNodes.Should().Be(1);
            response.NumberOfDataNodes.Should().Be(2);
            response.ActivePrimaryShards.Should().Be(3);
            response.ActiveShards.Should().Be(4);
            response.RelocatingShards.Should().Be(5);
            response.InitializingShards.Should().Be(6);
            response.UnassignedShards.Should().Be(7);
            response.DelayedUnassignedShards.Should().Be(8);
            response.NumberOfPendingTasks.Should().Be(9);
            response.NumberOfInFlightFetch.Should().Be(10);
            response.TaskMaxWaitTimeInQueueInMilliseconds.Should().Be(100);
            response.ActiveShardsPercentAsNumber.Should().Be(50.0);
        }
    }
}
