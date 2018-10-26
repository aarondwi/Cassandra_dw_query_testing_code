import pyspark_cassandra
import os
clear=lambda:os.system('clear')

cf_transaksi_standard = sqlContext.read.format('org.apache.spark.sql.cassandra').\
                options(table="cf_transaksi_standard", keyspace="skripsi").load()
cf_buku = sqlContext.read.format('org.apache.spark.sql.cassandra').\
                options(table="cf_buku", keyspace="skripsi").load()
cf_transaksi_jurusan_kategori = sqlContext.read.format('org.apache.spark.sql.cassandra').\
                options(table="cf_transaksi_jurusan_kategori", keyspace="skripsi").load()
mv_transaksi_kondisi_koleksi = sqlContext.read.format('org.apache.spark.sql.cassandra').\
                options(table="mv_transaksi_kondisi_koleksi", keyspace="skripsi").load()
cf_usulan = sqlContext.read.format('org.apache.spark.sql.cassandra').\
                options(table="cf_usulan", keyspace="skripsi").load()

#query 1
cf_transaksi_standard.registerTempTable("cf_transaksi_standard")
sqlContext.sql("""WITH cte as (
                    SELECT kode_anggota,
                           nama_jurusan,
                           fakultas,
                           tahun_ajaran, 
                           COUNT(unique_id) AS jumlah,
                           ROW_NUMBER() OVER (PARTITION BY tahun_ajaran ORDER BY COUNT(unique_id) DESC) AS r 
                    FROM cf_transaksi_standard
                    WHERE tahun_ajaran IN ('2014/2015','2015/2016','2016/2017')
                    GROUP BY kode_anggota,nama_jurusan,fakultas,tahun_ajaran
                )
                SELECT kode_anggota,nama_jurusan,fakultas,tahun_ajaran,jumlah FROM cte 
                WHERE r<=3 ORDER BY tahun_ajaran,jumlah DESC""").show()

#query 2
cf_transaksi_standard.registerTempTable("cf_transaksi_standard")
sqlContext.sql("""SELECT tahun_ajaran, YEAR(tanggal_pinjam), bulan, 
                         SUM(denda) AS Jumlah,
                         SUM(denda-terbayar) AS Belum_Terbayar
                    FROM cf_transaksi_standard
                    WHERE tahun_ajaran='2015/2016'
                    GROUP BY tahun_ajaran,YEAR(tanggal_pinjam),bulan
                    ORDER BY YEAR(tanggal_pinjam),bulan""").show()

#query 3
cf_buku.registerTempTable('cf_buku')
sqlContext.sql("""WITH cte AS (
                    SELECT Jumlah_Sekarang, bulan, tahun,
                    SUM(Jumlah_Sekarang) OVER (ORDER BY tahun,bulan) AS Total_Sekarang 
                    FROM (
                        SELECT tahun, bulan, COUNT(unique_id) AS Jumlah_Sekarang
                        FROM cf_buku
                        --WHERE tahun!=1
                        GROUP BY bulan, tahun
                    ) AS total_per_bulan_tahun
                    GROUP BY bulan, tahun, Jumlah_Sekarang
                )
                SELECT tahun, bulan, Total_Sekarang,
                Total_Sekarang - LAG(Total_Sekarang,1) OVER (PARTITION BY bulan ORDER BY tahun) AS Kenaikan
                FROM cte WHERE tahun BETWEEN 2014 AND 2016
                GROUP BY Total_Sekarang, bulan, tahun
                ORDER BY bulan, tahun""").show()

#query 4
cf_transaksi_jurusan_kategori.registerTempTable('cf_transaksi_jurusan_kategori')
sqlContext.sql("""SELECT judul,
                         nama_koleksi,
                         status_sekarang,
                         COUNT(unique_id) AS jumlah
                    FROM cf_transaksi_jurusan_kategori
                    WHERE nama_jurusan='Teknik Arsitektur'
                    AND tahun_ajaran IN ('2013/2014','2014/2015','2015/2016')
                    AND kelompok_kategori IN ('Geologi','Tata Kota dan Pertamanan')
                    GROUP BY judul,status_sekarang,nama_koleksi
                    ORDER BY COUNT(unique_id) DESC 
                    LIMIT 5""").show()

#query 5 
cf_transaksi_jurusan_kategori.registerTempTable('cf_transaksi_jurusan_kategori')
sqlContext.sql("""SELECT tahun_ajaran,
                         semester,
                         COUNT(unique_id) AS Jumlah,
                         COUNT(unique_id) - FLOOR(AVG(COUNT(unique_id)) 
                         OVER (PARTITION BY tahun_ajaran ORDER BY tahun_ajaran)) 
                    AS Selisih_dengan_Rata_rata_tahunan
                    FROM cf_transaksi_jurusan_kategori
                    WHERE nama_jurusan='Teknik Industri' AND tahun_ajaran IN ('2013/2014','2014/2015','2015/2016')
                    GROUP BY tahun_ajaran,semester
                    ORDER BY tahun_ajaran,semester""").show()

#query 6
mv_transaksi_kondisi_koleksi.registerTempTable('mv_transaksi_kondisi_koleksi')
sqlContext.sql("""SELECT kelompok_kategori, 
                         judul, 
                         kode_judul, 
                         kode_buku, 
                         COUNT(unique_id) AS Jumlah
                    FROM mv_transaksi_kondisi_koleksi
                    WHERE nama_koleksi IN ('Referensi','Laporan Kerja Praktek') 
                    AND tahun_ajaran IN ('2014/2015','2015/2016')
                    AND status_sekarang='hilang'
                    GROUP BY kelompok_kategori,judul,kode_judul,kode_buku
                    ORDER BY Jumlah DESC """).show()

#query 7
cf_usulan.registerTempTable("cf_usulan")
sqlContext.sql("""SELECT penerbit,
                         COUNT(unique_id) AS Jumlah
                    FROM cf_usulan 
                    WHERE tgl_usulan>'2013-01-01' AND 
                    status='Buku/AV Sedang Diolah' AND 
                    penerbit!='unknown' 
                    GROUP BY penerbit 
                    ORDER BY Jumlah DESC
                    LIMIT 10""").show()