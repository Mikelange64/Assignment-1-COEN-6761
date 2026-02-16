package coen448.computablefuture.test;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.RepeatedTest;

public class AsyncProcessorTest {   
    private static final int TIMEOUT_SECONDS = 2;
	@RepeatedTest(5)
    public void testProcessAsyncSuccess() throws ExecutionException, InterruptedException, TimeoutException {
        Microservice service1 = new Microservice("Hello");
        Microservice service2 = new Microservice("World");

        AsyncProcessor processor = new AsyncProcessor();
        CompletableFuture<String> resultFuture = processor.processAsync(
            List.of(service1, service2), "test");
        
        String result = resultFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("Hello:TEST World:TEST", result);
    }
	
	
	@ParameterizedTest
    @CsvSource({
        "hi, Hello:HI World:HI",
        "cloud, Hello:CLOUD World:CLOUD",
        "async, Hello:ASYNC World:ASYNC"
    })
    public void testProcessAsync_withDifferentMessages(
            String message,
            String expectedResult)
            throws ExecutionException, InterruptedException, TimeoutException {

        Microservice service1 = new Microservice("Hello");
        Microservice service2 = new Microservice("World");

        AsyncProcessor processor = new AsyncProcessor();

        CompletableFuture<String> resultFuture =
            processor.processAsync(List.of(service1, service2), message);

        String result = resultFuture.get(1, TimeUnit.SECONDS);

        assertEquals(expectedResult, result);
        
    }
	
	
	@RepeatedTest(20)
    void showNondeterminism_completionOrderVaries() throws Exception {

        Microservice s1 = new Microservice("A");
        Microservice s2 = new Microservice("B");
        Microservice s3 = new Microservice("C");

        AsyncProcessor processor = new AsyncProcessor();

        List<String> order = processor
            .processAsyncCompletionOrder(List.of(s1, s2, s3), "msg")
            .get(1, TimeUnit.SECONDS);

        // Not asserting a fixed order (because it is intentionally nondeterministic)
        System.out.println(order);

        // A minimal sanity check: all three must be present
        assertEquals(3, order.size());
   
        assertTrue(order.stream().anyMatch(x -> x.startsWith("A:")));
        assertTrue(order.stream().anyMatch(x -> x.startsWith("B:")));
        assertTrue(order.stream().anyMatch(x -> x.startsWith("C:")));
    }
    // ============================================================================
    // TASK A: FAIL-FAST POLICY TESTS
    // ============================================================================
    
    /**
     * Test: All services succeed
     * Expected: Normal completion with all results
     */
    @Test
    public void testFailFast_allServicesSucceed() throws Exception {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailFast(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("Service1:MSG1 Service2:MSG2 Service3:MSG3", output);
    }
    
    /**
     * Test: One service fails
     * Expected: Exception propagates, entire operation fails
     */
    @Test
    public void testFailFast_oneServiceFails() {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2", true, "Service2 failed");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailFast(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        // Must use assertThrows to verify exception propagation
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        });
        
        // Verify the underlying cause is our service failure
        assertNotNull(exception.getCause());
        assertTrue(exception.getCause().getMessage().contains("Service2 failed"));
    }
    
    /**
     * Test: First service fails immediately
     * Expected: Fast failure, no waiting for other services
     */
    @Test
    public void testFailFast_firstServiceFails() {
        Microservice s1 = new Microservice("Service1", true, "Service1 immediate failure");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailFast(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        });
        
        assertTrue(exception.getCause().getMessage().contains("Service1 immediate failure"));
    }
    
    /**
     * Test: Multiple services fail
     * Expected: At least one exception propagates
     */
    @Test
    public void testFailFast_multipleServicesFail() {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3", true, "Service3 failed");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailFast(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        });
        
        // At least one failure should propagate
        assertNotNull(exception.getCause());
    }
    
    /**
     * Test: Invalid input (mismatched list sizes)
     * Expected: Immediate failure with IllegalArgumentException
     */
    @Test
    public void testFailFast_invalidInput_mismatchedLists() {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailFast(
            List.of(s1, s2),
            List.of("msg1")  // Only one message for two services
        );
        
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        });
        
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }
}
	