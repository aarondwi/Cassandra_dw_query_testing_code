--query 1 (tested)
WITH cte as ( -- ~393.5s
	SELECT tf.kode_anggota,jd.nama_jurusan,jd.fakultas,dd.tahun_ajaran,
	COUNT(tf.transaksi_key) AS jumlah,
	ROW_NUMBER() OVER (PARTITION BY dd.tahun_ajaran ORDER BY count(tf.transaksi_key) DESC) AS r 
	FROM transaksi_fact tf
	JOIN date_dim dd ON tf.tgl_pinjam_key=dd.date_key
	JOIN jurusan_dim jd ON tf.jurusan_key=jd.jurusan_key
	WHERE dd.tahun_ajaran IN ('2014/2015' , '2015/2016' , '2016/2017')
	GROUP BY tf.kode_anggota,jd.nama_jurusan,jd.fakultas,dd.tahun_ajaran
)
SELECT kode_anggota,nama_jurusan,fakultas,tahun_ajaran,jumlah FROM cte 
WHERE r<=3 ORDER BY tahun_ajaran,jumlah DESC;

--query 1 (a speedier version)
WITH cte as ( -- ~354s
	SELECT tf.kode_anggota,dd.tahun_ajaran,tf.jurusan_key,
	COUNT(tf.transaksi_key) AS jumlah,
	ROW_NUMBER() OVER (PARTITION BY dd.tahun_ajaran ORDER BY count(tf.transaksi_key) DESC) AS r 
	FROM transaksi_fact tf
	JOIN date_dim dd ON tf.tgl_pinjam_key=dd.date_key
	WHERE dd.tahun_ajaran IN ('2014/2015' , '2015/2016' , '2016/2017')
	GROUP BY tf.kode_anggota,dd.tahun_ajaran,tf.jurusan_key
)
SELECT cte.kode_anggota, jd.nama_jurusan, jd.fakultas,
cte.tahun_ajaran, cte.jumlah
FROM cte
JOIN jurusan_dim jd ON cte.jurusan_key=jd.jurusan_key
WHERE cte.r<=3 
ORDER BY tahun_ajaran,jumlah DESC;

--query 2 (tested)
SELECT dd.tahun_ajaran,dd.tahun,dd.bulan,SUM(tf.denda) "Total Denda",(SUM(tf.denda)-SUM(tf.terbayar)) "Belum Terbayar"
FROM transaksi_fact tf
JOIN date_dim dd ON tf.tgl_pinjam_key=dd.date_key
WHERE dd.tahun_ajaran='2015/2016'
GROUP BY dd.tahun_ajaran,dd.tahun,dd.bulan
ORDER BY dd.tahun,dd.bulan;

--query 3 (tested)
WITH cte AS (
	SELECT "Jumlah Sekarang",bulan,tahun, 
	SUM("Jumlah Sekarang") OVER (ORDER BY tahun,bulan) AS "Total Sekarang" 
	FROM (
		SELECT COUNT(bf.buku_fact_key) AS "Jumlah Sekarang" ,dd.bulan,dd.tahun
		FROM buku_fact bf
		JOIN date_dim dd ON bf.tgl_datang_key=dd.date_key
		WHERE dd.tahun!=1
		GROUP BY dd.bulan,dd.tahun
	) AS total_per_bulan_tahun
	GROUP BY bulan,tahun,"Jumlah Sekarang"
)
SELECT "Total Sekarang",bulan,tahun, 
"Total Sekarang" - LAG("Total Sekarang",1) OVER (PARTITION BY bulan ORDER BY tahun) AS "Kenaikan"
FROM cte WHERE tahun BETWEEN 2014 AND 2016
GROUP BY "Total Sekarang",bulan,tahun
ORDER BY bulan,tahun;

--query 4 (tested)
SELECT bd.judul,bd.nama_koleksi,bd.status_sekarang,
COUNT(tf.transaksi_key) AS "Jumlah Peminjaman"
FROM buku_dim bd
JOIN transaksi_fact tf ON bd.buku_dim_key=tf.buku_dim_key
JOIN date_dim dd ON dd.date_key=tf.tgl_pinjam_key
JOIN jurusan_dim jd ON tf.jurusan_key=jd.jurusan_key
WHERE bd.kelompok_kategori IN ('Geologi','Tata Kota dan Pertamanan') 
AND dd.tahun_ajaran IN ('2013/2014','2014/2015','2015/2016') 
AND jd.nama_jurusan='Teknik Arsitektur' 
GROUP BY bd.judul,bd.status_sekarang,bd.nama_koleksi
ORDER BY COUNT(tf.transaksi_key) DESC
LIMIT 5;

--query 5 (tested)
SELECT dd.tahun_ajaran,dd.semester,COUNT(tf.transaksi_key) AS "Jumlah",
COUNT(tf.transaksi_key) - FLOOR(AVG(COUNT(tf.transaksi_key)) 
OVER (PARTITION BY dd.tahun_ajaran ORDER BY dd.tahun_ajaran)) AS "Selisih dengan Rata-rata tahunan"
FROM transaksi_fact tf
JOIN date_dim dd ON tf.tgl_pinjam_key=dd.date_key
JOIN jurusan_dim jd ON tf.jurusan_key=jd.jurusan_key
WHERE jd.nama_jurusan='Teknik Industri' AND dd.tahun_ajaran IN ('2013/2014','2014/2015','2015/2016')
GROUP BY dd.tahun_ajaran,dd.semester
ORDER BY dd.tahun_ajaran,dd.semester;

--query 6 (tested)
SELECT bd.kelompok_kategori,bd.judul,bd.kode_judul,bd.kode_buku,COUNT(tf.transaksi_key) AS Jumlah
FROM buku_dim bd
JOIN transaksi_fact tf ON bd.buku_dim_key=tf.buku_dim_key
JOIN date_dim dd ON dd.date_key=tf.tgl_pinjam_key
WHERE bd.nama_koleksi IN ('Referensi','Laporan Kerja Praktek') 
AND dd.tahun_ajaran IN ('2014/2015', '2015/2016')
AND status_sekarang='hilang'
GROUP BY bd.kelompok_kategori,bd.judul,bd.kode_judul,bd.kode_buku
ORDER BY COUNT(tf.transaksi_key) DESC;

--query 7 (tested)
SELECT uf.penerbit,COUNT(uf.*) AS "Jumlah"
FROM usulan_fact uf 
JOIN date_dim dd ON uf.tgl_key=dd.date_key 
WHERE dd.tanggal>'2013-01-01'::date AND uf.status='Buku/AV Sedang Diolah' AND uf.penerbit<>'unknown'
GROUP BY uf.penerbit
ORDER BY COUNT(uf.*) DESC
LIMIT 10;