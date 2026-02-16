package coen448.computablefuture.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class AsyncProcessor {
	
    public CompletableFuture<String> processAsync(List<Microservice> microservices, String message) {
    	
        List<CompletableFuture<String>> futures = microservices.stream()
            .map(client -> client.retrieveAsync(message))
            .collect(Collectors.toList());
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.joining(" ")));
        
    }
    
    public CompletableFuture<List<String>> processAsyncCompletionOrder(
            List<Microservice> microservices, String message) {

        List<String> completionOrder =
            Collections.synchronizedList(new ArrayList<>());

        List<CompletableFuture<Void>> futures = microservices.stream()
            .map(ms -> ms.retrieveAsync(message)
                .thenAccept(completionOrder::add))
            .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> completionOrder);
        
    }

    /**
     * TASK A: Fail-Fast Policy (Atomic)
     * 
     * If any microservice invocation fails, the entire operation fails immediately.
     * No partial result is produced. The exception propagates to the caller.
     * 
     * Use Case: Correctness-critical systems where partial results are invalid
     * (e.g., financial transactions, database operations requiring atomicity)
     * 
     * @param services List of microservices to invoke
     * @param messages List of messages, one per service (must match services list size)
     * @return CompletableFuture containing space-separated results from all services
     * @throws RuntimeException if any service fails (propagated through CompletableFuture)
     */
    public CompletableFuture<String> processAsyncFailFast(
            List<Microservice> services,
            List<String> messages) {
        
        if (services.size() != messages.size()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Services and messages lists must have the same size"));
        }
        
        // Create a future for each service-message pair
        List<CompletableFuture<String>> futures = java.util.stream.IntStream.range(0, services.size())
            .mapToObj(i -> services.get(i).retrieveAsync(messages.get(i)))
            .collect(Collectors.toList());
        
        // allOf() will fail fast if any future completes exceptionally
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)  // join() will propagate any exception
                .collect(Collectors.joining(" ")));
    }

    /**
     * TASK B: Fail-Partial Policy (Best-Effort)
     * 
     * Successful microservice invocations return results, while failed invocations 
     * are silently skipped. The operation completes normally with partial results.
     * 
     * Use Case: Dashboards, analytics, or aggregation where partial results are useful
     * (e.g., loading multiple widgets on a dashboard, some failures are acceptable)
     * 
     * Implementation Note: We use exceptionally() to handle failures per service,
     * returning null for failed services, then filter out nulls from final results.
     * 
     * @param services List of microservices to invoke
     * @param messages List of messages, one per service (must match services list size)
     * @return CompletableFuture containing list of successful results only
     */
    public CompletableFuture<List<String>> processAsyncFailPartial(
            List<Microservice> services,
            List<String> messages) {
        
        if (services.size() != messages.size()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Services and messages lists must have the same size"));
        }
        
        // Create futures that handle exceptions individually
        List<CompletableFuture<String>> futures = java.util.stream.IntStream.range(0, services.size())
            .mapToObj(i -> services.get(i).retrieveAsync(messages.get(i))
                .exceptionally(ex -> null))  // Convert failures to null
            .collect(Collectors.toList());
        
        // Wait for all futures (none will fail since we handle exceptions)
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .filter(result -> result != null)  // Filter out failed services
                .collect(Collectors.toList()));
    }

    /**
     * TASK C: Fail-Soft Policy (Fallback)
     * 
     * All failures are replaced with a predefined fallback value. 
     * The computation never fails and always produces a complete result.
     * 
     * Use Case: High-availability systems where degraded output is acceptable
     * (e.g., recommendation systems, content delivery, caching layers)
     * 
     * WARNING: This policy masks failures and may hide serious errors.
     * - Failures are not reported to the caller
     * - Debugging becomes harder as errors are silently replaced
     * - Service degradation may go unnoticed
     * - Should be combined with proper monitoring and alerting
     * 
     * @param services List of microservices to invoke
     * @param messages List of messages, one per service (must match services list size)
     * @param fallbackValue Value to use when a service fails
     * @return CompletableFuture containing space-separated results (with fallbacks for failures)
     */
    public CompletableFuture<String> processAsyncFailSoft(
            List<Microservice> services,
            List<String> messages,
            String fallbackValue) {
        
        if (services.size() != messages.size()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Services and messages lists must have the same size"));
        }
        
        // Create futures that replace exceptions with fallback value
        List<CompletableFuture<String>> futures = java.util.stream.IntStream.range(0, services.size())
            .mapToObj(i -> services.get(i).retrieveAsync(messages.get(i))
                .exceptionally(ex -> fallbackValue))  // Replace failure with fallback
            .collect(Collectors.toList());
        
        // All futures will complete normally (no exceptions)
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.joining(" ")));
    }
}