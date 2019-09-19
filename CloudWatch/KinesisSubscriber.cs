using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Newtonsoft.Json;

namespace LightestNight.System.Serverless.AWS.CloudWatch
{
    public abstract class KinesisSubscriber
    {
        private readonly string _prefix, _kinesisArn, _roleArn, _filterName, _filterPattern, _shipperFunctionName;
        private readonly int _retentionDays;

        private readonly AmazonCloudWatchLogsClient _cloudWatchLogs;

        protected KinesisSubscriber(string region, string prefix, string kinesisArn, string roleArn, string filterName, string filterPattern, string shipperFunctionName, int retentionDays)
        {
            _prefix = prefix;
            _kinesisArn = kinesisArn;
            _roleArn = roleArn;
            _filterName = filterName;
            _filterPattern = filterPattern;
            _shipperFunctionName = shipperFunctionName;
            _retentionDays = retentionDays;
            
            _cloudWatchLogs = new AmazonCloudWatchLogsClient(RegionEndpoint.GetBySystemName(region));
        }

        protected async Task Subscribe()
        {
            var logGroups = await GetLogGroups();
            await SubscribeAll(logGroups);
        }

        private async Task<IEnumerable<string>> GetLogGroups(IEnumerable<string> logGroups = null, string nextToken = default)
        {
            while (true)
            {
                if (logGroups == null)
                    logGroups = new string[0];

                var request = new DescribeLogGroupsRequest {Limit = 50, LogGroupNamePrefix = _prefix, NextToken = nextToken};
                var result = await _cloudWatchLogs.DescribeLogGroupsAsync(request);

                logGroups = logGroups.Concat(result.LogGroups.Select(logGroup => logGroup.LogGroupName));

                if (!string.IsNullOrEmpty(result.NextToken))
                    nextToken = result.NextToken;
                else
                    return logGroups;
            }
        }

        private async Task SubscribeAll(IEnumerable<string> logGroups)
        {
            foreach (var logGroup in logGroups)
            {
                if (logGroup.EndsWith(_shipperFunctionName))
                {
                    Console.WriteLine($"Skipping [{logGroup}] because it will create cyclic events from it's own logs.");
                    continue;
                }
                
                Console.WriteLine($"Subscribing [{logGroup}]");
                await SubscribeLogGroup(logGroup);
                
                Console.WriteLine($"Updating retention policy to [{_retentionDays} days] for [{logGroup}].");
                await SetRetentionPolicy(logGroup);
            }
        }

        private async Task SubscribeLogGroup(string logGroup)
        {
            var request = new PutSubscriptionFilterRequest
            {
                DestinationArn = _kinesisArn,
                LogGroupName = logGroup,
                FilterName = _filterName,
                FilterPattern = _filterPattern,
                RoleArn = _roleArn,
                Distribution = "ByLogStream"
            };

            try
            {
                await _cloudWatchLogs.PutSubscriptionFilterAsync(request);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to Subscribe [{logGroup}].");
                await Console.Error.WriteLineAsync(JsonConvert.SerializeObject(ex));

                await UpsertSubscriptionFilter(request);
            }
        }

        private async Task UpsertSubscriptionFilter(PutSubscriptionFilterRequest request)
        {
            Console.WriteLine("Upserting Subscription Filter...");

            var subscriptionFilters = await _cloudWatchLogs.DescribeSubscriptionFiltersAsync(new DescribeSubscriptionFiltersRequest
            {
                LogGroupName = request.LogGroupName
            });
            var subscriptionFilter = subscriptionFilters.SubscriptionFilters.First();

            if (subscriptionFilter.FilterName != request.FilterName || subscriptionFilter.FilterPattern != request.FilterPattern)
            {
                await Task.WhenAll(_cloudWatchLogs.DeleteSubscriptionFilterAsync(new DeleteSubscriptionFilterRequest
                {
                    FilterName = subscriptionFilter.FilterName,
                    LogGroupName = request.LogGroupName
                }), _cloudWatchLogs.PutSubscriptionFilterAsync(request));
            }
        }

        private Task SetRetentionPolicy(string logGroup)
            => _cloudWatchLogs.PutRetentionPolicyAsync(new PutRetentionPolicyRequest
            {
                LogGroupName = logGroup,
                RetentionInDays = _retentionDays
            });
    }
}