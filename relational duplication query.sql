--transaksi_fact
CREATE TABLE transaksi_fact_dummy AS SELECT id_transaksi,buku_dim_key, tgl_pinjam_key, tgl_batas_key,tgl_kembali_key, jurusan_key, kode_anggota, denda, terbayar FROM transaksi_fact;

INSERT INTO transaksi_fact (id_transaksi,buku_dim_key, tgl_pinjam_key, tgl_batas_key,tgl_kembali_key, jurusan_key, kode_anggota, denda, terbayar) SELECT * FROM transaksi_fact_dummy;

--buku_fact
CREATE TABLE buku_fact_dummy AS SELECT buku_dim_key,tgl_datang_key,tgl_input_key FROM buku_fact;

INSERT INTO buku_fact (buku_dim_key,tgl_datang_key,tgl_input_key) SELECT * FROM buku_fact_dummy;

--usulan_fact
CREATE TABLE usulan_fact_dummy AS SELECT id_usul, tgl_key, jurusan_key, kode_anggota, judul, penerbit, pengarang, jenis_usul, status FROM usulan_fact;

INSERT INTO usulan_fact (id_usul, tgl_key, jurusan_key, kode_anggota, judul, penerbit, pengarang, jenis_usul, status) SELECT * FROM usulan_fact_dummy;