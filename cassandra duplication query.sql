create table cf_transaksi_standard_dummy(
	tahun_ajaran text,
    bulan int,
	tanggal_pinjam date,
	tanggal_batas date,
	tanggal_kembali date,
	unique_id uuid,
	semester int,
	id_transaksi int,
	denda double,
	terbayar double,
	nama_jurusan text,
	fakultas text,
	kode_anggota text,
	primary key((tahun_ajaran,bulan),tanggal_pinjam,tanggal_batas,tanggal_kembali,unique_id)
);
COPY cf_transaksi_standard(tahun_ajaran, bulan, tanggal_pinjam, tanggal_batas, tanggal_kembali, unique_id, semester,  id_transaksi, denda, terbayar, nama_jurusan, fakultas, kode_anggota) TO './cf_transaksi_standard.csv';
COPY cf_transaksi_standard_dummy(tahun_ajaran, bulan, tanggal_pinjam, tanggal_batas, tanggal_kembali, unique_id, semester,  id_transaksi, denda, terbayar, nama_jurusan, fakultas, kode_anggota) FROM './cf_transaksi_standard.csv';

create table cf_buku_dummy(
	tahun int,
    bulan int,
	tanggal_datang date,
	tanggal_input date,
	unique_id uuid,
	kode_buku text,
	kode_judul int,
	judul text,
	nama_koleksi text,
	jenis_terbitan text,
	penerbit text,
	kelompok_kategori text,
	status_lama text,
	tanggal_ganti_status date,
	status_sekarang text,
	primary key((tahun,bulan),tanggal_datang,tanggal_input,unique_id)
);
COPY cf_buku(tahun, bulan, tanggal_datang, tanggal_input, unique_id, kode_buku, kode_judul, judul, nama_koleksi, jenis_terbitan, penerbit, kelompok_kategori, status_lama, tanggal_ganti_status, status_sekarang) TO './cf_buku.csv';
COPY cf_buku_dummy(tahun, bulan, tanggal_datang, tanggal_input, unique_id, kode_buku, kode_judul, judul, nama_koleksi, jenis_terbitan, penerbit, kelompok_kategori, status_lama, tanggal_ganti_status, status_sekarang) FROM './cf_buku.csv';

create table cf_transaksi_jurusan_kategori_dummy(
	nama_jurusan text,
	tahun_ajaran text,
	tgl_pinjam date,
	kelompok_kategori text,
	nama_koleksi text,
	unique_id uuid,
	semester int,
	id_transaksi int,
	fakultas text,
	kode_anggota text,
	tgl_batas date,
	tgl_kembali date,
	kode_judul int,
	kode_buku text,
	judul text,
	status_lama text,
	tanggal_ganti_status date,
	status_sekarang text,
	denda double,
	terbayar double,
	primary key ((nama_jurusan,tahun_ajaran),tgl_pinjam,kelompok_kategori,nama_koleksi,unique_id)
);
COPY cf_transaksi_jurusan_kategori(nama_jurusan, tahun_ajaran, tgl_pinjam, kelompok_kategori, nama_koleksi, unique_id, semester, id_transaksi, fakultas, kode_anggota, tgl_batas, tgl_kembali, kode_judul, kode_buku, judul, status_lama, tanggal_ganti_status, status_sekarang, denda, terbayar) TO './cf_transaksi_jurusan_kategori.csv';
COPY cf_transaksi_jurusan_kategori_dummy(nama_jurusan, tahun_ajaran, tgl_pinjam, kelompok_kategori, nama_koleksi, unique_id, semester, id_transaksi, fakultas, kode_anggota, tgl_batas, tgl_kembali, kode_judul, kode_buku, judul, status_lama, tanggal_ganti_status, status_sekarang, denda, terbayar) FROM './cf_transaksi_jurusan_kategori.csv';

create table cf_usulan_dummy(
	tahun int,
    bulan int,
	tgl_usulan date,
	unique_id uuid,
	id_usulan int,
	nama_jurusan text,
	kode_anggota text,
	fakultas text,
	penerbit text,
	judul text,
	pengarang text,
	jenis_usul text,
	status text,
	primary key((tahun,bulan),tgl_usulan,unique_id)
);
COPY cf_usulan(tahun, bulan, tgl_usulan, unique_id, id_usulan, nama_jurusan, kode_anggota, fakultas, penerbit, judul, pengarang, jenis_usul, status) TO './cf_usulan.csv';
COPY cf_usulan_dummy(tahun, bulan, tgl_usulan, unique_id, id_usulan, nama_jurusan, kode_anggota, fakultas, penerbit, judul, pengarang, jenis_usul, status) FROM './cf_usulan.csv';