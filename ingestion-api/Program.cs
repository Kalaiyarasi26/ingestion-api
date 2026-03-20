using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.IO;
using CsvHelper;
using System.Globalization;
using Microsoft.Data.Sqlite;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddAntiforgery(options =>
{
    options.SuppressXFrameOptionsHeader = true;
});

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


app.MapPost("/products/upload", async (HttpRequest request) =>
{
    // Handle file upload logic here
    var form = await request.ReadFormAsync();
    string dbpath = "/Users/kalai/Documents/Projects/ingestion-api/ingestion-api/ingestion.db";
    var files = form.Files;
    Console.WriteLine(files);
    
    if (files.Count  == 0)
    {
        return Results.BadRequest("No file uploaded");
    }
      var firstFile= files[0];
    if (firstFile.Length == 0)
    {
        return Results.BadRequest("File is empty");
    }
   
    var stream =  firstFile.OpenReadStream();
    var reader = new StreamReader(stream);
    var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    csv.Read();
    var header = csv.ReadHeader();
    var rowsProcessed = 0;
    string connectionString = "Data Source=" + dbpath;
        
        using (var connection = new SqliteConnection(connectionString))
    {
        try
        {
            connection.Open();
            while (csv.Read()){

                rowsProcessed ++;
                var ProductID= csv.GetField("product_id");
                var ProductName= csv.GetField("product_name");
                var DiscountedPrice= csv.GetField("discounted_price");
                DiscountedPrice = DiscountedPrice.Replace(",", "");
                var ActualPrice= csv.GetField("actual_price");
                ActualPrice = ActualPrice.Replace(",", "");
                var Rating= csv.GetField("rating");
                var RatingCount= csv.GetField("rating_count");
                RatingCount = RatingCount.Replace(",", "");
                Console.WriteLine($"ProductID: {ProductID}, ProductName: {ProductName}, DiscountedPrice: {DiscountedPrice}, ActualPrice: {ActualPrice}, Rating: {Rating}, RatingCount: {RatingCount}");

                // Convert numeric values safely
                string productIdValue = string.IsNullOrWhiteSpace(ProductID)
                    ? "Unknown"
                    : ProductID;

                string productNameValue = string.IsNullOrWhiteSpace(ProductName)
                    ? "Unknown"
                    : ProductName;
                double discountedPriceValue;
                double.TryParse(DiscountedPrice, out discountedPriceValue);

                double actualPriceValue;
                double.TryParse(ActualPrice, out actualPriceValue);

                double ratingValue;
                double.TryParse(Rating, out ratingValue);

                int ratingCountValue;
                int.TryParse(RatingCount, out ratingCountValue);

                 var insertSql = @"INSERT INTO products_raw
                (product_id, product_name, discounted_price, actual_price, rating, rating_count)
                VALUES
                (@product_id, @product_name, @discounted_price, @actual_price, @rating, @rating_count)";
                var command = new SqliteCommand(insertSql, connection);

                command.Parameters.AddWithValue("@product_id", productIdValue);
                command.Parameters.AddWithValue("@product_name", productNameValue);
                command.Parameters.AddWithValue("@discounted_price", discountedPriceValue);
                command.Parameters.AddWithValue("@actual_price", actualPriceValue);
                command.Parameters.AddWithValue("@rating", ratingValue);
                command.Parameters.AddWithValue("@rating_count", ratingCountValue);

                command.ExecuteNonQuery();
                            
            }
        }
         catch(SqliteException ex)
        {
            Console.WriteLine("Error connecting to database: " + ex.Message);
        }
    }
   return Results.Ok(new { rowsProcessed });
});

app.Run();

record Product(String product_id,  String product_name, double discounted_price,double actual_price ,double rating, int rating_count);

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}

