using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

//app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast =  Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast");

app.MapGet("/products", () =>
{
     string dbpath = "/Users/kalai/Documents/Projects/ingestion-api/ingestion-api/ingestion.db";
     
     var sql="SELECT * FROM products_raw";
      // DEBUG: check if the file exists and its full path
        Console.WriteLine("DB file exists? " + File.Exists(dbpath));
        Console.WriteLine("DB full path: " + new FileInfo(dbpath).FullName);
        Console.Out.Flush();
         List<Product> products = new List<Product>();
    
        string connectionString = "Data Source=" + dbpath;
        
        using (var connection = new SqliteConnection(connectionString))
    {
        try
        {
            connection.Open();
            Console.WriteLine("Connection to database established successfully.");
            var reader = new SqliteCommand(sql, connection).ExecuteReader();
            Console.Out.Flush();

            while(reader.Read()){
               
                Console.WriteLine("product_name: {0}, actual_price: {1}",reader["product_name"],reader["actual_price"] );
                
                products.Add(new Product(reader["product_id"].ToString() ?? "Unknown", reader["product_name"].ToString()?? "Unknown", reader["discounted_price"] != DBNull.Value ? Convert.ToDouble(reader["discounted_price"]):0.0 , 
                reader["actual_price"] != DBNull.Value ? Convert.ToDouble(reader["actual_price"]) : 0.0 , reader["rating"] != DBNull.Value ? Convert.ToDouble(reader["rating"]) : 0.0 ,reader["rating_count"] != DBNull.Value ? Convert.ToInt32(reader["rating_count"]):0 ));
            }
        }
        catch(SqliteException ex)
        {
            Console.WriteLine("Error connecting to database: " + ex.Message);
        }
    }
    Console.WriteLine("Database connection closed!");
    return  products;
});

app.Run();

record Product(String product_id,  String product_name, double discounted_price,double actual_price ,double rating, int rating_count);

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}

