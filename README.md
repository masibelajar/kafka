# Rama Owarianto Putra Suharjito (5027231049)

# Apache Kafka + PySpark PBL
Simulasi pemantauan real-time kondisi gudang menggunakan Kafka dan PySpark. Data dari sensor suhu dan kelembaban dikirim secara real-time dan dianalisis untuk mendeteksi potensi bahaya terhadap barang sensitif.

## Langkah Menjalankan

### 1. Jalankan Kafka
Pastikan Kafka dan Zookeeper sudah berjalan.

### 2. Buat Topik Kafka
```
bash create_topics.sh
```
### 3. Jalankan Kafka Producer

Di terminal terpisah:
```python producers/suhu_producer.py```
```python producers/kelembaban_producer.py```

### 4. Jalankan PySpark Consumer
```spark-submit consumer/pyspark_consumer.py```

### Dependencies
- Python 3
- Apache Kafka
- PySpark
- kafka-python (pip install kafka-python)
