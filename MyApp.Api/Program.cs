using Microsoft.EntityFrameworkCore;
using MyApp.Data.Context;

var builder = WebApplication.CreateBuilder(args);

// Додаємо сервіси
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Database
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

// Kafka
builder.Services.AddSingleton<KafkaProducer>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();

// Конфігурація pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();
app.MapControllers();

app.Run();