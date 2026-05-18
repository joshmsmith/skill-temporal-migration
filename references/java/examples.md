# Java Migration Examples

Side-by-side code translations for Java developers migrating to Temporal. Primary source tools covered: **Camunda 7**, **Camunda 8 / Zeebe**, **Quartz Scheduler**, and **TIBCO BusinessWorks**.

For SDK setup and Worker configuration, refer to the `temporal-developer` skill (`references/java/java.md`).

*Verified against: Temporal Java SDK 1.x, Camunda 7.21, Camunda 8.4, Quartz 2.3.x*

---

## Camunda 7: Java Delegate → Temporal Activity

```java
// ── CAMUNDA 7 ────────────────────────────────────────────────────────────────
// Service Task implementation: Java Delegate
public class CheckInventoryDelegate implements JavaDelegate {
    @Autowired
    private InventoryService inventoryService;

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String orderId = (String) execution.getVariable("orderId");
        int quantity = (int) execution.getVariable("quantity");

        boolean inStock = inventoryService.isAvailable(orderId, quantity);
        execution.setVariable("inStock", inStock);  // write back to process variables
    }
}


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// Activity interface
@ActivityInterface
public interface InventoryActivities {
    @ActivityMethod
    boolean checkInventory(String orderId, int quantity);
}

// Activity implementation
@Component
public class InventoryActivitiesImpl implements InventoryActivities {
    @Autowired
    private InventoryService inventoryService;

    @Override
    public boolean checkInventory(String orderId, int quantity) {
        return inventoryService.isAvailable(orderId, quantity);
        // Return value instead of writing to execution variables
    }
}
```

---

## Camunda 7: Full Process → Temporal Workflow

```java
// ── CAMUNDA 7 ────────────────────────────────────────────────────────────────
// Process definition lives in BPMN XML (order-process.bpmn)
// Deployed via: repositoryService.createDeployment().addClasspathResource("order-process.bpmn").deploy()
// Started via: runtimeService.startProcessInstanceByKey("orderProcess", variables)
//
// Process flow (from BPMN):
//   Start → CheckInventory [XOR] → {inStock=true} → ChargePayment → ShipOrder → End
//                                → {inStock=false} → NotifyOutOfStock → End


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
@WorkflowInterface
public interface OrderWorkflow {
    @WorkflowMethod
    OrderResult processOrder(OrderRequest request);
}

public class OrderWorkflowImpl implements OrderWorkflow {
    private final OrderActivities activities = Workflow.newActivityStub(
        OrderActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofMinutes(5))
            .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
            .build()
    );

    @Override
    public OrderResult processOrder(OrderRequest request) {
        // Exclusive Gateway: replaces BPMN XOR gateway + sequence flow conditions
        boolean inStock = activities.checkInventory(request.getOrderId(), request.getQuantity());
        if (!inStock) {
            activities.notifyOutOfStock(request.getOrderId());
            return OrderResult.outOfStock();
        }
        activities.chargePayment(request.getOrderId(), request.getAmount());
        activities.shipOrder(request.getOrderId());
        return OrderResult.success();
    }
}

// Start a workflow execution (replaces runtimeService.startProcessInstanceByKey):
WorkflowClient client = WorkflowClient.newInstance(WorkflowServiceStubs.newLocalServiceStubs());
OrderWorkflow workflow = client.newWorkflowStub(
    OrderWorkflow.class,
    WorkflowOptions.newBuilder()
        .setWorkflowId("order-" + orderId)
        .setTaskQueue("order-processing")
        .build()
);
OrderResult result = workflow.processOrder(request);
```

---

## Camunda 7: Message Intermediate Event → Signal

```java
// ── CAMUNDA 7 ────────────────────────────────────────────────────────────────
// BPMN: Message Intermediate Catch Event ("PaymentReceived") pauses the instance
// Resume via: runtimeService.correlateMessage("PaymentReceived", businessKey, variables)


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
@WorkflowInterface
public interface OrderWorkflow {
    @WorkflowMethod
    OrderResult processOrder(OrderRequest request);

    @SignalMethod
    void paymentReceived(PaymentData payment);  // ← replaces Message Catch Event
}

public class OrderWorkflowImpl implements OrderWorkflow {
    private PaymentData paymentData = null;

    @Override
    public void paymentReceived(PaymentData payment) {
        this.paymentData = payment;
    }

    @Override
    public OrderResult processOrder(OrderRequest request) {
        activities.reserveInventory(request.getOrderId());

        // Wait for payment signal (equivalent to Message Intermediate Catch Event)
        Workflow.await(() -> paymentData != null);

        activities.fulfillOrder(request.getOrderId(), paymentData);
        return OrderResult.success();
    }
}

// Send the signal from your payment service (replaces runtimeService.correlateMessage):
WorkflowStub stub = client.newUntypedWorkflowStub("order-" + orderId);
stub.signal("paymentReceived", new PaymentData(transactionId, amount));
```

---

## Camunda 7: Timer Intermediate Event → Workflow.sleep

```java
// ── CAMUNDA 7 ────────────────────────────────────────────────────────────────
// BPMN: Timer Intermediate Catch Event — wait PT3D (3 days)


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
public class SubscriptionWorkflowImpl implements SubscriptionWorkflow {
    @Override
    public void run(Subscription subscription) {
        activities.sendWelcomeEmail(subscription.getUserId());
        Workflow.sleep(Duration.ofDays(3));        // ← replaces Timer Intermediate Event
        activities.sendTrialEndingReminder(subscription.getUserId());
        Workflow.sleep(Duration.ofDays(4));
        activities.promptUpgrade(subscription.getUserId());
    }
}
```

---

## Camunda 8 / Zeebe: Job Worker → Temporal Worker + Activity

```java
// ── CAMUNDA 8 / ZEEBE ────────────────────────────────────────────────────────
ZeebeClient zeebeClient = ZeebeClient.newClientBuilder().build();

// Register job worker for type "check-inventory"
zeebeClient.newWorker()
    .jobType("check-inventory")
    .handler((client, job) -> {
        Map<String, Object> variables = job.getVariablesAsMap();
        String orderId = (String) variables.get("orderId");

        boolean inStock = inventoryService.isAvailable(orderId);
        client.newCompleteCommand(job.getKey())
            .variables(Map.of("inStock", inStock))
            .send();
    })
    .open();


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// Activity interface (replaces jobType string)
@ActivityInterface
public interface InventoryActivities {
    @ActivityMethod
    boolean checkInventory(String orderId);
}

// Activity implementation (replaces job handler lambda)
public class InventoryActivitiesImpl implements InventoryActivities {
    @Override
    public boolean checkInventory(String orderId) {
        return inventoryService.isAvailable(orderId);
        // Return value instead of calling client.newCompleteCommand()
        // Throw exception instead of calling client.newFailCommand()
    }
}

// Worker registration (replaces ZeebeClient job worker)
Worker worker = workerFactory.newWorker("order-processing");
worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);
worker.registerActivitiesImplementations(new InventoryActivitiesImpl());
workerFactory.start();
```

---

## Quartz Scheduler: Job → Schedule + Activity

```java
// ── QUARTZ ───────────────────────────────────────────────────────────────────
// Job class
public class GenerateReportJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String reportType = context.getMergedJobDataMap().getString("reportType");
        reportService.generate(reportType);
        emailService.send(reportType);
    }
}

// Scheduling the job
JobDetail jobDetail = JobBuilder.newJob(GenerateReportJob.class)
    .withIdentity("generateReport", "reports")
    .usingJobData("reportType", "daily-summary")
    .build();

Trigger trigger = TriggerBuilder.newTrigger()
    .withSchedule(CronScheduleBuilder.cronSchedule("0 0 8 * * ?"))  // 8am daily
    .build();

scheduler.scheduleJob(jobDetail, trigger);


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
// Activities (replace the Job execute() method — one activity per unit of work)
@ActivityInterface
public interface ReportActivities {
    @ActivityMethod
    void generateReport(String reportType);

    @ActivityMethod
    void sendReport(String reportType);
}

// Workflow (orchestrates the activities)
@WorkflowInterface
public interface ReportWorkflow {
    @WorkflowMethod
    void run(String reportType);
}

public class ReportWorkflowImpl implements ReportWorkflow {
    private final ReportActivities activities = Workflow.newActivityStub(
        ReportActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofHours(1))
            .build()
    );

    @Override
    public void run(String reportType) {
        activities.generateReport(reportType);
        activities.sendReport(reportType);
    }
}

// Schedule (replaces CronTrigger + scheduler.scheduleJob)
ScheduleClient scheduleClient = ScheduleClient.newInstance(serviceStubs);
scheduleClient.createSchedule(
    "generate-daily-report",
    Schedule.newBuilder()
        .setAction(
            ScheduleActionStartWorkflow.newBuilder()
                .setWorkflowType(ReportWorkflow.class)
                .setArguments("daily-summary")
                .setOptions(WorkflowOptions.newBuilder()
                    .setTaskQueue("reports")
                    .build())
                .build()
        )
        .setSpec(ScheduleSpec.newBuilder()
            .addCronExpression("0 8 * * *")  // 8am daily
            .build())
        .build(),
    ScheduleOptions.getDefaultInstance()
);
```

---

## TIBCO BusinessWorks: Java Process with JDBC and HTTP → Workflow + Activities

```java
// ── TIBCO BW (conceptual) ────────────────────────────────────────────────────
// BW Process:
//   HTTP Receiver (starter) → JDBC Query (fetch order) → HTTP Client (call payment API)
//   → JDBC Update (mark paid) → End


// ── TEMPORAL ─────────────────────────────────────────────────────────────────
@ActivityInterface
public interface OrderActivities {
    @ActivityMethod
    Order fetchOrder(String orderId);          // replaces JDBC Query activity

    @ActivityMethod
    PaymentResult chargePayment(String orderId, long amount);  // replaces HTTP Client activity

    @ActivityMethod
    void markOrderPaid(String orderId, String chargeId);  // replaces JDBC Update activity
}

@WorkflowInterface
public interface OrderProcessingWorkflow {
    @WorkflowMethod
    void processOrder(String orderId);
}

public class OrderProcessingWorkflowImpl implements OrderProcessingWorkflow {
    private final OrderActivities activities = Workflow.newActivityStub(
        OrderActivities.class,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofMinutes(5))
            .setRetryOptions(RetryOptions.newBuilder()
                .setMaximumAttempts(3)
                .setInitialInterval(Duration.ofSeconds(10))
                .build())
            .build()
    );

    @Override
    public void processOrder(String orderId) {
        Order order = activities.fetchOrder(orderId);
        PaymentResult charge = activities.chargePayment(orderId, order.getAmount());
        activities.markOrderPaid(orderId, charge.getChargeId());
    }
}

// The HTTP Receiver (BW Starter) is replaced by a REST controller:
@RestController
public class OrderController {
    private final WorkflowClient workflowClient;

    @PostMapping("/orders/{orderId}/process")
    public ResponseEntity<Void> processOrder(@PathVariable String orderId) {
        WorkflowOptions options = WorkflowOptions.newBuilder()
            .setWorkflowId("order-" + orderId)
            .setTaskQueue("order-processing")
            .build();
        workflowClient.newWorkflowStub(OrderProcessingWorkflow.class, options)
            .processOrder(orderId);
        return ResponseEntity.accepted().build();
    }
}
```

---

## Compensation / Saga Pattern (replaces BPMN Compensation Events)

```java
// ── TEMPORAL: Saga pattern ────────────────────────────────────────────────────
public class BookingWorkflowImpl implements BookingWorkflow {
    private final BookingActivities activities = Workflow.newActivityStub(
        BookingActivities.class,
        ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofMinutes(5)).build()
    );

    @Override
    public void book(BookingRequest request) {
        List<Runnable> compensations = new ArrayList<>();

        try {
            activities.bookFlight(request);
            compensations.add(0, () -> activities.cancelFlight(request));

            activities.bookHotel(request);
            compensations.add(0, () -> activities.cancelHotel(request));

            activities.chargeCard(request);
            compensations.add(0, () -> activities.refundCard(request));

        } catch (ActivityFailure e) {
            // Run compensations in reverse order
            compensations.forEach(Runnable::run);
            throw e;
        }
    }
}
```
