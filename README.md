# Streaming-data-twitter
Project ini tentang implementasi dari Streaming Data yang bersumber dari Twitter. Data tersebut berupa Tweet, user id serta tangggal tweet tersebut dibuat.  Dalam streaming data dari twitter tersebut, digunakan library Apache Kafka menyimpan nya dalam database phpmyadmin

Twitter data stream.py berisikan program untuk terhubung dengan twitter api dan melakukan streaming menggunakan kafka
ETL proses.py berikan program untuk melakukan proses ekstraksi dari consumer kafka, transform untuk merubah data agar sesuai dengan skema yang telah ditentukan sehingga dapat dilakukan load kedalam phpmyadmin
Dump data.py berisikan program untuk mengambil seluruh data yang telah di load untuk dijadikan sebagai file csv
