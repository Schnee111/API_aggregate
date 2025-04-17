from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, avg
import os

app = Flask(__name__)
CORS(app)

# MongoDB connection
client = MongoClient("mongodb+srv://coffeelatte:secretdata3@luna.sryzase.mongodb.net/")
db = client["bigdata_saham"]
collection = db["yfinance_data"]

# Spark session
spark = SparkSession.builder.appName("SahamApp").getOrCreate()

# Helper: Load and flatten MongoDB data into Spark DataFrame
def load_data(emiten):
    raw = list(collection.find({"info.symbol": emiten}))
    if not raw:
        return None

    # Ambil history dan tambahkan field symbol
    data = []
    for doc in raw:
        symbol = doc['info']['symbol']
        for record in doc['info']['history']:
            record['symbol'] = symbol
            data.append(record)

    df = spark.createDataFrame(data)
    df = df.withColumn("Date", to_date("Date"))
    return df

# Endpoint: Daftar emiten
@app.route("/api/emiten")
def get_emiten():
    symbols = collection.distinct("info.symbol")
    return jsonify(symbols)

# Endpoint: Harga saham per emiten dan periode
@app.route("/api/harga")
def get_harga():
    emiten = request.args.get("emiten")
    period = request.args.get("period", "daily")

    df = load_data(emiten)
    if df is None:
        return jsonify({"error": "Emiten not found"}), 404

    # Agregasi berdasarkan periode
    if period == "monthly":
        df = df.withColumn("period", date_format("Date", "yyyy-MM"))
    elif period == "yearly":
        df = df.withColumn("period", date_format("Date", "yyyy"))
    else:
        df = df.withColumn("period", col("Date"))

    agg_df = df.groupBy("period").agg(
        avg("Open").alias("Open"),
        avg("High").alias("High"),
        avg("Low").alias("Low"),
        avg("Close").alias("Close"),
        avg("Volume").alias("Volume")
    ).orderBy("period")

    result = [
        {
            "Date": row["period"],
            "Open": round(row["Open"], 2),
            "High": round(row["High"], 2),
            "Low": round(row["Low"], 2),
            "Close": round(row["Close"], 2),
            "Volume": int(row["Volume"])
        }
        for row in agg_df.collect()
    ]

    return jsonify(result)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
