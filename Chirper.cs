using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System;
using Microsoft.DurableTask.Entities;
using DurableTask.Core.Entities;
using System.Net;
using System.Threading;
using Azure.Core;
using Microsoft.DurableTask.Client.Entities;

namespace DurableEntitiesDemo
{








    public struct Chirp
    {
        public string UserId { get; set; }

        public DateTime Timestamp { get; set; }

        public string Content { get; set; }
    }



    public class UserChirps
    { 
        public List<Chirp> Chirps { get; set; } = new List<Chirp>();

        public void Add(Chirp chirp)
        {
            Chirps.Add(chirp);
        }

        public void Remove(DateTime timestamp)
        {
            int i=Chirps.RemoveAll(chirp => chirp.Timestamp == timestamp);
            Console.WriteLine($"Removed {i} chirps");
        }

        public Task<List<Chirp>> Get()
        {
            return Task.FromResult(Chirps);
        }

        // Boilerplate (entry point for the functions runtime)
        [Function(nameof(UserChirps))]
        public static Task HandleEntityOperation([EntityTrigger] TaskEntityDispatcher context)
        {
            return context.DispatchAsync<UserChirps>();
        }
    }

 
    public class UserFollows 
    {

        public List<string> FollowedUsers { get; set; } = new List<string>();

        public void Add(string user)
        {
            FollowedUsers.Add(user);
        }

        public void Remove(string user)
        {
            FollowedUsers.Remove(user);
        }

        public Task<List<string>> Get()
        {
            return Task.FromResult(FollowedUsers);
        }

        // Boilerplate (entry point for the functions runtime)
        [Function(nameof(UserFollows))]
        public static Task HandleEntityOperation([EntityTrigger] TaskEntityDispatcher context)
        {
            return context.DispatchAsync<UserFollows>();
        }
    }



    public static class Chirper
    {
        [Function("GetTimeline")]
        public static async Task<Chirp[]> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(Chirper));
            logger.LogInformation("Saying hello.");

            var userId = context.GetInput<string>();

            var entityId = new EntityInstanceId(nameof(UserFollows), userId);

            // call the UserFollows entity to figure out whose chirps should be included
            var followedUsers = context.Entities.CallEntityAsync<List<string>>(entityId, "Get");
  

            // in parallel, collect all the chirps from the followedUsers
            var tasks = new List<Task<List<Chirp>>>();
            foreach (var followedUser in await followedUsers)
            {
                var chirpsEntityId = new EntityInstanceId(nameof(UserChirps), followedUser);
                tasks.Add(context.Entities.CallEntityAsync<List<Chirp>>(chirpsEntityId, "Get"));
            }

    




            await Task.WhenAll(tasks);

            // combine and sort the returned lists of chirps
            var sortedResults = tasks
                .SelectMany(task => task.Result)
                .OrderBy(chirp => chirp.Timestamp);

            return sortedResults.ToArray();


        }

        [Function(nameof(SayHello))]
        public static string SayHello([ActivityTrigger] string name, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("SayHello");
            logger.LogInformation("Saying hello to {name}.", name);
            return $"Hello {name}!";
        }

        [Function("Chirper_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("Chirper_HttpStart");

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(Chirper));

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            // Returns an HTTP 202 response with an instance management payload.
            // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }

        [Function("UserChirpsGet")]
        public static async Task<HttpResponseData> UserChirpsGet(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "user/{userId}/chirps")] HttpRequestData req,
               [DurableClient] DurableTaskClient client,
            string userId,
             FunctionContext executionContext)
        {
            //  Authenticate(req, userId);
            ILogger logger = executionContext.GetLogger("UserChirpsGet");
            var entityId = new EntityInstanceId(nameof(UserChirps), userId);
            EntityMetadata<UserChirps>? entity = await client.Entities.GetEntityAsync<UserChirps>(entityId);

            if (entity is null)
            {
                return req.CreateResponse(HttpStatusCode.NotFound);
            }

            HttpResponseData response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(entity);

            return response;
        }

        [Function("UserChirpsPost")]
        public static async Task<HttpResponseData> UserChirpsPost(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "user/{userId}/chirps")] HttpRequestData req,
              [DurableClient] DurableTaskClient client,
           string userId,
            FunctionContext executionContext)
        {
            //  Authenticate(req, userId);
            ILogger logger = executionContext.GetLogger("UserChirpsPost");

            var chirp = new Chirp()
            {
                UserId = userId,
                Timestamp = DateTime.UtcNow,
                Content = await req.ReadAsStringAsync(),
            };


            var entityId = new EntityInstanceId(nameof(UserChirps), userId);
            EntityMetadata<UserChirps>? entity = await client.Entities.GetEntityAsync<UserChirps>(entityId);

            await client.Entities.SignalEntityAsync(entityId,"Add",chirp);

            HttpResponseData response = req.CreateResponse(HttpStatusCode.Accepted);
            await response.WriteAsJsonAsync(chirp);

            return response;
            //return req.CreateResponse(HttpStatusCode.Accepted, chirp);
        }



        [Function("UserChirpsDelete")]
        public static async Task<HttpResponseData> UserChirpsDelete(
            [HttpTrigger(AuthorizationLevel.Function, "delete", Route = "user/{userId}/chirps/{timestamp}")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            ILogger log,
            string userId,
            DateTime timestamp,
            FunctionContext executionContext)
        {
            // Authenticate(req, userId);
            var entityId = new EntityInstanceId(nameof(UserChirps), userId);
            EntityMetadata<UserChirps>? entity = await client.Entities.GetEntityAsync<UserChirps>(entityId);

            await client.Entities.SignalEntityAsync(entityId,"Remove",timestamp);
            return req.CreateResponse(HttpStatusCode.Accepted);
        }

        [Function("UserFollowsGet")]
        public static async Task<HttpResponseData> UserFollowsGet(
           [HttpTrigger(AuthorizationLevel.Function, "get", Route = "user/{userId}/follows")] HttpRequestData req,
           [DurableClient] DurableTaskClient client,
        FunctionContext executionContext,
           string userId)
        {
      
            var entityId = new EntityInstanceId(nameof(UserFollows), userId);
            EntityMetadata<UserFollows>? entity = await client.Entities.GetEntityAsync<UserFollows>(entityId);

            if (entity is null)
            {
                return req.CreateResponse(HttpStatusCode.NotFound);
            }

            HttpResponseData response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(entity);
            return response;

    
        }


        [Function("UserFollowsPost")]
        public static async Task<HttpResponseData> UserFollowsPost(
          [HttpTrigger(AuthorizationLevel.Function, "post", Route = "user/{userId}/follows/{userId2}")] HttpRequestData req,
          [DurableClient] DurableTaskClient client,
          FunctionContext executionContext,
          string userId,
          string userId2)
        {
            // Authenticate(req, userId);

            var entityId = new EntityInstanceId(nameof(UserFollows), userId);
            EntityMetadata<UserFollows>? entity = await client.Entities.GetEntityAsync<UserFollows>(entityId);
            EntityMetadata<UserFollows>? entity2 = await client.Entities.GetEntityAsync<UserFollows>(entityId);

            await client.Entities.SignalEntityAsync(entityId,"Add",userId2);
            return req.CreateResponse(HttpStatusCode.Accepted);
        }


        [Function("UserFollowsDelete")]
        public static async Task<HttpResponseData> UserFollowsDelete(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "user/{userId}/follows/{userId2}")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
FunctionContext executionContext,
        string userId,
        string userId2)
        {

            var content = await req.ReadAsStringAsync();
            var entityId = new EntityInstanceId(nameof(UserChirps), userId);

            await client.Entities.SignalEntityAsync(entityId,"Remove", userId);
            return req.CreateResponse(HttpStatusCode.Accepted);
        }


        [Function("UserTimelineGet")]
        public static async Task<HttpResponseData> UserTimelineGet(
         [HttpTrigger(AuthorizationLevel.Function, "get", Route = "user/{userId}/timeline")] HttpRequestData req,
         [DurableClient] DurableTaskClient client,
      FunctionContext executionContext,
         string userId)
        {

            var instanceId = await client.ScheduleNewOrchestrationInstanceAsync("GetTimeLine", userId);
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
            //return await client.WaitForCompletionOrCreateCheckStatusResponseAsync(req, instanceId);
        }
    }




}
