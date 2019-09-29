using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon.Lambda.KinesisEvents;
using Humanizer;
using LightestNight.System.Logging;
using LightestNight.System.Serverless.Kinesis;
using LightestNight.System.Utilities.Extensions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace LightestNight.System.Serverless.AWS.CloudWatch
{
    public abstract class KinesisShipper
    {
        private readonly Logger _logger;

        protected KinesisShipper(Logger logger)
        {
            _logger = logger;
        }

        protected virtual async Task Ship(KinesisEvent @event)
        {
            try
            {
                var logs = (await ParseLogs(@event)).ToArray();
                if (logs.Any())
                {
                    await Task.WhenAll(logs.Select(log => _logger(log)));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(JsonConvert.SerializeObject(ex));
            }
        }
        
        private static async Task<IEnumerable<LogData>> ParseLogs(KinesisEvent @event)
        {
            var logs = new List<LogData>();

            foreach (var record in @event.Records)
            {
                var kinesisRecord = record.Kinesis;
                string payload;
                using (var decompressionStream = new GZipStream(kinesisRecord.Data, CompressionMode.Decompress))
                {
                    using (var streamReader = new StreamReader(decompressionStream))
                        payload = await streamReader.ReadToEndAsync();
                }

                var data = JsonConvert.DeserializeObject<Payload>(payload);
                if (data.MessageType == "CONTROL_MESSAGE")
                    continue;

                var functionName = GetLambdaName(data.LogGroup);
                var functionVersion = GetLambdaVersion(data.LogStream);
                var awsRegion = record.AwsRegion;
                
                logs.AddRange(data.LogEvents.Select(logEvent => ParseLog(functionName, functionVersion, logEvent.Message, awsRegion))
                    .Where(log => log != null));
            }

            return logs;
        }

        private static LogData ParseLog(string functionName, string functionVersion, string message, string region)
        {
            if (message.StartsWith("START RequestId", StringComparison.InvariantCultureIgnoreCase) ||
                message.StartsWith("END RequestId", StringComparison.InvariantCultureIgnoreCase) ||
                message.StartsWith("REPORT RequestId", StringComparison.InvariantCultureIgnoreCase))
                return null;

            const string type = "Lambda";
            const string structuredLogPattern = "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T(2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z([ \t])[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}([ \t])(.*)";
            const string exceptionStarter = "Exception: {";
            var regexError = new Regex("error", RegexOptions.IgnoreCase);
            var regexException = new Regex("exception", RegexOptions.IgnoreCase);
            var regexConfigurationError = new Regex("module initialization error|unable to import module", RegexOptions.IgnoreCase);
            var regexTimeoutError = new Regex("task timed out|process exited before completing", RegexOptions.IgnoreCase);
            var regexStructuredLog = new Regex(structuredLogPattern);

            LogData CheckLogError(LogData log)
            {
                if (regexException.IsMatch(message) && message.StartsWith(exceptionStarter, StringComparison.InvariantCultureIgnoreCase))
                {
                    log.Title = $"A {ErrorType.Runtime.Humanize()} error occurred in {log.Function}";
                    log.Severity = LogLevel.Critical;
                    log.ErrorType = ErrorType.Runtime;
                    log.Exception = message.ExtractObject<Exception>();
                } else if (regexError.IsMatch(log.Message))
                {
                    log.Title = $"A {ErrorType.Runtime.Humanize()} error occurred in {log.Function}";
                    log.Severity = LogLevel.Error;
                    log.ErrorType = ErrorType.Runtime;
                } else if (regexConfigurationError.IsMatch(log.Message))
                {
                    log.Title = $"A {ErrorType.Configuration.Humanize()} error occurred in {log.Function}";
                    log.Severity = LogLevel.Error;
                    log.ErrorType = ErrorType.Configuration;
                } else if (regexTimeoutError.IsMatch(log.Message))
                {
                    log.Title = $"A {ErrorType.Timeout.Humanize()} error occurred in {log.Function}";
                    log.Severity = LogLevel.Error;
                    log.ErrorType = ErrorType.Timeout;
                }

                return log;
            }
            
            if (!regexStructuredLog.IsMatch(message))
            {
                return CheckLogError(new LogData
                {
                    Message = message,
                    Function = functionName,
                    FunctionVersion = functionVersion,
                    Region = region,
                    Type = type,
                    Severity = LogLevel.Information,
                    Timestamp = DateTimeOffset.Now.ToUnixTimeSeconds()
                });
            }

            (long Timestamp, string RequestId, string Message) SplitStructuredLog(string msg)
            {
                var parts = msg.Split(new[] {"\t"}, 3, StringSplitOptions.None);
                if (!long.TryParse(parts[0], out var logTimestamp))
                    logTimestamp = DateTimeOffset.Now.ToUnixTimeSeconds();

                return (logTimestamp, parts[1], parts[2]);
            }

            var (timestamp, requestId, s) = SplitStructuredLog(message);
            return CheckLogError(new LogData
            {
                Message = s,
                Function = functionName,
                FunctionVersion = functionVersion,
                Region = region,
                Type = type,
                Severity = LogLevel.Information,
                Timestamp = timestamp,
                RequestId = requestId
            });
        }

        private static string GetLambdaName(string logGroup)
            => logGroup.Split('/').ToArray().Reverse().First();

        private static string GetLambdaVersion(string logStream)
            => logStream.Substring(logStream.IndexOf("[", StringComparison.Ordinal) + 1, logStream.IndexOf("]", StringComparison.Ordinal));
    }
}