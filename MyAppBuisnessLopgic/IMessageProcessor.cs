using Microsoft.Extensions.Logging;

public interface IMessageProcessor
{
    Task ProcessTransferAsync(string message);
    Task ProcessUserCreationAsync(string message);
}

public class MessageProcessor : IMessageProcessor
{
    private readonly ILogger<MessageProcessor> _logger;

    public MessageProcessor(ILogger<MessageProcessor> logger)
    {
        _logger = logger;
    }

    public async Task ProcessTransferAsync(string message)
    {
        // Складна бізнес-логіка обробки трансферів
        _logger.LogInformation($"Processing transfer: {message}");
        await Task.Delay(100);
    }

    public async Task ProcessUserCreationAsync(string message)
    {
        // Складна бізнес-логіка створення користувачів
        _logger.LogInformation($"Processing user creation: {message}");
        await Task.Delay(100);
    }
}