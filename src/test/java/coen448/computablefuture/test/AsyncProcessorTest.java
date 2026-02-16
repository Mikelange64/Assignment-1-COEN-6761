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
    // ============================================================================
    // TASK B: FAIL-PARTIAL POLICY TESTS
    // ============================================================================
    
    /**
     * Test: All services succeed
     * Expected: All results returned in list
     */
    @Test
    public void testFailPartial_allServicesSucceed() throws Exception {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<List<String>> result = processor.processAsyncFailPartial(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        List<String> output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(3, output.size());
        assertTrue(output.contains("Service1:MSG1"));
        assertTrue(output.contains("Service2:MSG2"));
        assertTrue(output.contains("Service3:MSG3"));
    }
    
    /**
     * Test: One service fails
     * Expected: Only successful results returned, no exception
     */
    @Test
    public void testFailPartial_oneServiceFails() throws Exception {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2", true, "Service2 failed");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<List<String>> result = processor.processAsyncFailPartial(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        // Should complete normally (no exception)
        List<String> output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        
        // Only 2 successful results
        assertEquals(2, output.size());
        assertTrue(output.contains("Service1:MSG1"));
        assertTrue(output.contains("Service3:MSG3"));
        assertFalse(output.stream().anyMatch(s -> s.startsWith("Service2:")));
    }
    
    /**
     * Test: First service fails
     * Expected: Remaining successful results returned
     */
    @Test
    public void testFailPartial_firstServiceFails() throws Exception {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<List<String>> result = processor.processAsyncFailPartial(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        List<String> output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        
        assertEquals(2, output.size());
        assertTrue(output.contains("Service2:MSG2"));
        assertTrue(output.contains("Service3:MSG3"));
    }
    
    /**
     * Test: Multiple services fail
     * Expected: Only successful results returned
     */
    @Test
    public void testFailPartial_multipleServicesFail() throws Exception {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3", true, "Service3 failed");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<List<String>> result = processor.processAsyncFailPartial(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        List<String> output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        
        // Only Service2 succeeds
        assertEquals(1, output.size());
        assertEquals("Service2:MSG2", output.get(0));
    }
    
    /**
     * Test: All services fail
     * Expected: Empty list returned, no exception
     */
    @Test
    public void testFailPartial_allServicesFail() throws Exception {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2", true, "Service2 failed");
        Microservice s3 = new Microservice("Service3", true, "Service3 failed");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<List<String>> result = processor.processAsyncFailPartial(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3")
        );
        
        // Should complete normally with empty list
        List<String> output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, output.size());
    }
    
    /**
     * Test: Invalid input (mismatched list sizes)
     * Expected: Exception
     */
    @Test
    public void testFailPartial_invalidInput_mismatchedLists() {
        Microservice s1 = new Microservice("Service1");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<List<String>> result = processor.processAsyncFailPartial(
            List.of(s1),
            List.of("msg1", "msg2")  // More messages than services
        );
        
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        });
        
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }

    // ============================================================================
    // TASK C: FAIL-SOFT POLICY TESTS
    // ============================================================================
    
    /**
     * Test: All services succeed
     * Expected: All results returned, no fallback used
     */
    @Test
    public void testFailSoft_allServicesSucceed() throws Exception {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3"),
            "FALLBACK"
        );
        
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("Service1:MSG1 Service2:MSG2 Service3:MSG3", output);
        assertFalse(output.contains("FALLBACK"));
    }
    
    /**
     * Test: One service fails
     * Expected: Fallback value used for failed service, operation completes normally
     */
    @Test
    public void testFailSoft_oneServiceFails() throws Exception {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2", true, "Service2 failed");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3"),
            "FALLBACK"
        );
        
        // Should complete normally (no exception)
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        
        assertEquals("Service1:MSG1 FALLBACK Service3:MSG3", output);
        assertTrue(output.contains("FALLBACK"));
    }
    
    /**
     * Test: First service fails
     * Expected: Fallback in first position
     */
    @Test
    public void testFailSoft_firstServiceFails() throws Exception {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3"),
            "UNAVAILABLE"
        );
        
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("UNAVAILABLE Service2:MSG2 Service3:MSG3", output);
    }
    
    /**
     * Test: Multiple services fail
     * Expected: Multiple fallback values in result
     */
    @Test
    public void testFailSoft_multipleServicesFail() throws Exception {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2");
        Microservice s3 = new Microservice("Service3", true, "Service3 failed");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3"),
            "ERROR"
        );
        
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("ERROR Service2:MSG2 ERROR", output);
    }
    
    /**
     * Test: All services fail
     * Expected: All fallback values, operation completes normally
     */
    @Test
    public void testFailSoft_allServicesFail() throws Exception {
        Microservice s1 = new Microservice("Service1", true, "Service1 failed");
        Microservice s2 = new Microservice("Service2", true, "Service2 failed");
        Microservice s3 = new Microservice("Service3", true, "Service3 failed");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1, s2, s3),
            List.of("msg1", "msg2", "msg3"),
            "N/A"
        );
        
        // Should complete normally even though all services failed
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("N/A N/A N/A", output);
    }
    
    /**
     * Test: Custom fallback values
     * Expected: Custom fallback used appropriately
     */
    @Test
    public void testFailSoft_customFallbackValue() throws Exception {
        Microservice s1 = new Microservice("Service1");
        Microservice s2 = new Microservice("Service2", true, "Service2 failed");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1, s2),
            List.of("data", "data"),
            "SERVICE_DEGRADED"
        );
        
        String output = result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals("Service1:DATA SERVICE_DEGRADED", output);
    }
    
    /**
     * Test: Invalid input (mismatched list sizes)
     * Expected: Exception
     */
    @Test
    public void testFailSoft_invalidInput_mismatchedLists() {
        Microservice s1 = new Microservice("Service1");
        
        AsyncProcessor processor = new AsyncProcessor();
        
        CompletableFuture<String> result = processor.processAsyncFailSoft(
            List.of(s1),
            List.of(),  // Empty messages list
            "FALLBACK"
        );
        
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        });
        
        assertTrue(exception.getCause() instanceof IllegalArgumentException);
    }
}