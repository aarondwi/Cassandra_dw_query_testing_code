create table date_dim(
	date_key serial primary key,
	tanggal date not null,
	hari integer not null,
	bulan integer not null,
	tahun integer not null,
	tahun_ajaran text not null,
	semester integer not null
)
distribute by replication;

insert into date_dim (date_key, tanggal,hari,bulan,tahun,tahun_ajaran,semester) values (0,'01-01-1900'::date,EXTRACT(DAY from '01-01-1900'::date),EXTRACT(MONTH from '01-01-1900'::date), EXTRACT(YEAR from '01-01-1900'::date),'None',0);

insert into date_dim(tanggal,hari,bulan,tahun,tahun_ajaran,semester) select 
	day,
	EXTRACT(DAY from day),
	EXTRACT(MONTH from day), 
	EXTRACT(YEAR from day), 
	case when EXTRACT(MONTH from day)<=6 then 
	concat((EXTRACT(YEAR from day)-1)::text,'/',EXTRACT(YEAR from day)::text) else 
	concat(EXTRACT(YEAR from day)::text,'/',(EXTRACT(YEAR from day)+1)::text) end,
	case when EXTRACT(MONTH from day)<=6 then 2 else 1 end 
	from generate_series('1970-01-01'::date, '2016-12-31'::date, '1 day') day;

--create table penerbit_dim(
--	penerbit_key serial primary key,
--	kode_penerbit integer,
--	penerbit text)
--distribute by replication;
--insert into penerbit_dim values (0,0,'unknown');

create table jurusan_dim(
	jurusan_key serial primary key,
	kode_jurusan integer not null,
	nama_jurusan text not null,
	fakultas text not null
)
distribute by replication;
insert into jurusan_dim values(0,0,"karyawan","karyawan");

create table usulan_fact(
	usulan_key serial primary key,
	id_usul integer not null,
	tgl_key integer not null,
	jurusan_key integer not null,
	kode_anggota text not null,
	judul text not null,
	penerbit text,
	pengarang text,
	jenis_usul text not null,
	status text not null,
	constraint usul_fact_tgl_ref foreign key(tgl_key)
		references date_dim(date_key) match simple,
	constraint usul_fact_jurusan_ref foreign key(jurusan_key)
		references jurusan_dim(jurusan_key) match simple
)
distribute by hash(usulan_key);

create table buku_dim(
	buku_dim_key serial primary key,
	kode_judul integer  not null,
	kode_buku text not null,
	judul text,
	nama_koleksi text not null,
	jenis_terbitan text,
	kelompok_kategori text not null,
	penerbit text not null,
	status_lama text,
	tanggal_status_ganti date,
	status_sekarang text not null
)
distribute by replication;

create table buku_fact(
	buku_fact_key serial primary key,
	buku_dim_key integer not null,
	tgl_datang_key integer not null,
	tgl_input_key integer not null,
	constraint buku_fact_buku_dim_ref foreign key(buku_dim_key)
		references buku_dim(buku_dim_key) match simple,
	constraint buku_fact_tgl_datang_date_ref foreign key(tgl_datang_key)
		references date_dim(date_key) match simple,
	constraint buku_fact_tgl_input_date_ref foreign key(tgl_input_key)
		references date_dim(date_key) match simple
)
distribute by hash(buku_fact_key);

create table transaksi_fact(
	transaksi_key serial primary key,
	id_transaksi integer not null,
	buku_dim_key integer not null,
	tgl_pinjam_key integer not null,
	tgl_batas_key integer not null,
	tgl_kembali_key integer not null,
	jurusan_key integer not null,
	kode_anggota text not null,
	denda numeric,
	terbayar numeric,
	constraint transaksi_fact_buku_dim_ref foreign key(buku_dim_key)
		references buku_dim(buku_dim_key) match simple,
	constraint transaksi_fact_tgl_pinjam_ref foreign key(tgl_pinjam_key)
		references date_dim(date_key) match simple,
	constraint transaksi_fact_tgl_batas_ref foreign key(tgl_batas_key)
		references date_dim(date_key) match simple,
	constraint transaksi_fact_tgl_kembali_ref foreign key(tgl_kembali_key)
		references date_dim(date_key) match simple
)
distribute by hash(transaksi_key);