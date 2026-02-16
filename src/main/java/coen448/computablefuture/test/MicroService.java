package coen448.computablefuture.test;


import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class Microservice {
	
    private final String serviceId;
    private final boolean shouldFail;
    private final String failureMessage;

    /**
     * Creates a microservice that always succeeds.
     * @param serviceId Unique identifier for this service
     */
    public Microservice(String serviceId) {
        this(serviceId, false, null);
    }
    
    /**
     * Creates a microservice with configurable failure behavior.
     * @param serviceId Unique identifier for this service
     * @param shouldFail If true, this service will always fail
     * @param failureMessage Custom exception message (used only if shouldFail is true)
     */
    public Microservice(String serviceId, boolean shouldFail, String failureMessage) {
        this.serviceId = serviceId;
        this.shouldFail = shouldFail;
        this.failureMessage = failureMessage != null ? failureMessage : "Service " + serviceId + " failed";
    }

//    public CompletableFuture<String> retrieveAsync(String input) {
//        // include input in the output so tests can verify the passed message
//        return CompletableFuture.supplyAsync(() -> serviceId + ":" + input.toUpperCase());
//    }
    public CompletableFuture<String> retrieveAsync(String input) {
        return CompletableFuture.supplyAsync(() -> {
            // jitter: 0..30ms to perturb scheduling
            int delayMs = ThreadLocalRandom.current().nextInt(0, 31);
            try {
                TimeUnit.MILLISECONDS.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            
            // Simulate failure if configured
            if (shouldFail) {
                throw new RuntimeException(failureMessage);
            }
            
            return serviceId + ":" + input.toUpperCase();
            //return serviceId + ":" + input.toUpperCase() + "(" + delayMs + "ms)";
        });
    }
    
}