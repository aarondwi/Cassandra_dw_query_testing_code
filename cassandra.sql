create keyspace skripsi with replication={'class':'SimpleStrategy','replication_factor':1};

create table cf_transaksi_standard(
	tanggal_pinjam date,
	tanggal_batas date,
	tanggal_kembali date,
	unique_id uuid,
	semester int,
	tahun_ajaran text,
	id_transaksi int,
	denda double,
	terbayar double,
	nama_jurusan text,
	fakultas text,
	kode_anggota text,
	primary key(tanggal_pinjam,tanggal_batas,tanggal_kembali,unique_id)
) with gc_grace_seconds=1;

create table cf_buku(
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
	primary key(tanggal_datang,tanggal_input,unique_id)
) with gc_grace_seconds=1;

create table cf_transaksi_jurusan_kategori(
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
) with gc_grace_seconds=1;

create materialized view mv_transaksi_kondisi_koleksi 
as select * from cf_transaksi_jurusan_kategori where 
nama_koleksi is not null and 
tahun_ajaran is not null and 
tgl_pinjam is not null and 
status_sekarang is not null and 
nama_jurusan is not null and 
kelompok_kategori is not null and 
unique_id is not null
primary key((nama_koleksi,tahun_ajaran),tgl_pinjam,
status_sekarang,nama_jurusan,kelompok_kategori,unique_id);

create table cf_usulan(
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
	primary key(tgl_usulan,unique_id)
) with gc_grace_seconds=1;