create table koleksi(fckd_jsnkol text primary key,k099l text);
\copy koleksi from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\koleksi.csv' with (format csv);

create table jnstbt(fnkd_jsntbt bigint primary key, fcket_jnstbt text);
\copy jnstbt from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\jnstbt.csv' with (format csv);

create table status_usul(kode_status bigint primary key, nama_status text);
\copy status_usul from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\status_usul.csv' with delimiter '|' (format csv);

create table lokasi(fnkd_lokasi int primary key, fcket_lokasi text);
\copy lokasi from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\lokasi.csv' with (format csv);

create table penerbit(fnkd_penerbit int primary key, k260b text);
\copy penerbit from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\penerbit.csv' with delimiter '|';

create table kategori(k099a text primary key,nama_kategori text);
\copy kategori from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\kategori.csv' with (format csv);

/*create table judul(knokat bigint primary key, k099a double precision, k099b text, k099c text, fnkd_jsntbt int, k245a text, fnkd_penerbit int default null);
\copy judul from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\judul.csv' delimiter '|' with '' AS NULL;

create table buku(fnkd_jsntbt bigint primary key, fcket_jnstbt text);
\copy buku from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\buku.csv' with (format csv);

create table transaksi(fnkd_jsntbt bigint primary key, fcket_jnstbt text);
\copy transaksi from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\transaksi.csv' with (format csv);

create table usul(fnkd_jsntbt bigint primary key, fcket_jnstbt text);
\copy usul from 'C:\Users\Aaron\Desktop\UKP\skripsi\Pembuatan Skripsi\csv_awal\usul.csv' with (format csv);*/