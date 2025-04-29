using Marten;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using Weasel.Core;
using Wolverine;
using Wolverine.Attributes;
using Wolverine.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMarten(options =>
{
    // Establish the connection string to your Marten database
    options.Connection("Host=localhost;Database=marten_test;Username=postgres;Password=postgres");

    // Specify that we want to use STJ as our serializer
    options.UseSystemTextJsonForSerialization();

    options.Projections.Add<TestProjection>(ProjectionLifecycle.Inline);

    options.Events.EnableSideEffectsOnInlineProjections = true;

    if (builder.Environment.IsDevelopment())
    {
        options.AutoCreateSchemaObjects = AutoCreate.All;
    }
});

builder.UseWolverine(opts =>
{
    opts.PublishMessagesToRabbitMqExchange<FooMessage>("test.exchange", x => "bar");
    opts.ListenToRabbitQueue(
            "test.queue",
            x =>
            {
                x.BindExchange("test.exchange", "bar");
            }
        )
        .UseDurableInbox();

    opts.AutoBuildMessageStorageOnStartup = true;
    opts.ApplicationAssembly = typeof(Program).Assembly;
    Console.WriteLine(opts.DescribeHandlerMatch(typeof(FooMessageHandlers)));

    opts.UseRabbitMq(x =>
        {
            x.HostName = "localhost";
        })
        .AutoProvision();
});

var app = builder.Build();

app.MapGet("/", () => "Hello World!");
app.MapGet(
    "/change",
    async (IDocumentStore store) =>
    {
        await using var session = store.LightweightSession();

        session.Events.StartStream<Test>(Guid.NewGuid(), new TestEvent { Foo = "Hello" });

        await session.SaveChangesAsync();

        return Results.Ok("Changed!");
    }
);

app.MapGet(
    "/send",
    async (IMessageBus bus) =>
    {
        var message = new FooMessage("Hello World!");
        await bus.PublishAsync(message);

        return Results.Ok("Sent!");
    }
);

app.Run();

public class Test
{
    public Guid Id { get; set; }

    public string Foo { get; set; }
}

public class TestEvent
{
    public string Foo { get; set; }
}

public class TestProjection : SingleStreamProjection<Test>
{
    public void Apply(Test test, TestEvent testEvent)
    {
        test.Foo = testEvent.Foo;
    }

    public override ValueTask RaiseSideEffects(
        IDocumentOperations operations,
        IEventSlice<Test> slice
    )
    {
        Console.WriteLine($"Raising side effects");
        slice.PublishMessage(new FooMessage("Hello World!"));

        return default;
    }
}

public class FooMessage(string Foo)
{
    public string Foo { get; set; } = Foo;
}

[WolverineHandler]
public class FooMessageHandlers
{
    public void Handle(FooMessage message)
    {
        // Do something with the message
        Console.WriteLine($"Received message: {message.Foo}");
    }
}
