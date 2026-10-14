# BÁO CÁO THỰC HÀNH HỆ THỐNG PHÂN TÁN HADOOP HDFS & MAPREDUCE

---

## I. THIẾT LẬP MÔI TRƯỜNG & KHỞI CHẠY HADOOP SINGLE-NODE

### 1. Cài đặt các thành phần phụ thuộc
- **Java JDK**: Đã cấu hình và cài đặt `OpenJDK-17-JDK-Headless` nhằm đáp ứng tốt nhất yêu cầu môi trường của Apache Hadoop 3.5.0.
- **SSH Server**: Cài đặt `openssh-server`, khởi tạo các cặp khóa Host Keys (`ssh-keygen -A`) và kích hoạt dịch vụ chạy ngầm. Thiết lập cơ chế xác thực không mật khẩu (Passwordless SSH) qua địa chỉ localhost `127.0.0.1`.
- **Hadoop**: Tải phiên bản ổn định **Apache Hadoop 3.5.0** và cấu hình các biến môi trường hệ thống chính:
  - `JAVA_HOME` và `HADOOP_HOME` được gán chính xác trong môi trường hệ thống và tệp tin cấu hình `/content/hadoop-3.5.0/etc/hadoop/hadoop-env.sh`.

### 2. Cấu hình các dịch vụ chính (XML Configs)
- **`core-site.xml`**: Cấu hình Hệ thống tập tin mặc định kết nối trực tiếp đến NameNode tại địa chỉ HDFS cục bộ:
  ```xml
  <property>
      <name>fs.defaultFS</name>
      <value>hdfs://localhost:9000</value>
  </property>
  ```
- **`hdfs-site.xml`**: Thiết lập hệ số nhân bản dữ liệu (Replication Factor) bằng 1 (Single Node) và định tuyến các thư mục dữ liệu cục bộ thực tế chứa NameNode & DataNode:
  ```xml
  <property>
      <name>dfs.replication</name>
      <value>1</value>
  </property>
  <property>
      <name>dfs.namenode.name.dir</name>
      <value>file:///content/hadoop_data/namenode</value>
  </property>
  <property>
      <name>dfs.datanode.data.dir</name>
      <value>file:///content/hadoop_data/datanode</value>
  </property>
  ```
- **Khởi tạo và Chạy Daemon**:
  - Thực hiện định dạng NameNode: `hdfs namenode -format -force`.
  - Khởi động các tiến trình ngầm NameNode và DataNode.
  - Sử dụng lệnh `jps` để kiểm chứng và nhận thấy cả hai dịch vụ nền quan trọng đã hoạt động bình thường.

---

## II. QUẢN LÝ DỮ LIỆU TRÊN HDFS

### 1. Khởi tạo dữ liệu giao dịch cục bộ (`transactions.txt`)
Tạo file dữ liệu mẫu gồm 20 giao dịch theo định dạng `Branch,TransactionType,Amount` mô tả các giao dịch của các chi nhánh ngân hàng (Hanoi, HCM, Danang, CanTho) với hai loại hình: `DEPOSIT` (Gửi tiền) và `WITHDRAW` (Rút tiền).

### 2. Thao tác và kiểm tra lưu trữ trên HDFS
- **Tạo thư mục đầu vào**: `hdfs dfs -mkdir -p /user/student/transactions/input`
- **Đưa dữ liệu lên HDFS**: Dùng lệnh `hdfs dfs -put` để đẩy tệp tin từ môi trường Colab lên hệ thống phân tán.
- **Kiểm tra lưu trữ**:
  - Sử dụng lệnh `hdfs dfs -ls /user/student/transactions/input` xác nhận tệp tin tồn tại.
  - Sử dụng lệnh `hdfs dfs -cat /.../transactions.txt` để đọc kiểm tra tính toàn vẹn của dữ liệu.
  - Sử dụng lệnh `hdfs fsck` để kiểm tra phân mảnh khối dữ liệu. Kết quả ghi nhận tệp tin có dung lượng nhỏ nên được gom trong **1 block duy nhất** (`blk_1073741829`), trạng thái hệ thống tệp tin đạt chuẩn **HEALTHY**.
  - Sử dụng lệnh `hdfs dfsadmin -report` và `hdfs dfs -du -h` để đo đạc dung lượng sử dụng, nhận định hệ thống phân tán đang sử dụng hoàn toàn ổn định.

---

## III. PHÂN TÍCH MAPREDUCE (HADOOP STREAMING)

### 1. Thiết kế giải thuật phân tích tổng doanh số giao dịch theo chi nhánh
Chúng ta thiết lập một quy trình xử lý luồng MapReduce phân tán bằng ngôn ngữ Python để tính tổng toàn bộ lượng tiền giao dịch phát sinh (`Amount`) theo từng chi nhánh (`Branch`).

- **Mapper (`mapper_trans.py`)**: Đọc từng dòng dữ liệu đầu vào từ luồng vào tiêu chuẩn (`sys.stdin`), lọc bỏ dòng tiêu đề và phân tách các trường thông tin bằng ký tự dấu phẩy `,`. Trích xuất và in ra màn hình cặp Khóa - Giá trị: `Branch\tAmount`.
- **Reducer (`reducer_trans.py`)**: Nhận dữ liệu đầu vào đã được Hadoop sắp xếp theo Khóa (`Branch`). Thực hiện cộng dồn toàn bộ lượng tiền giao dịch `Amount` cho đến khi kết thúc khóa và xuất ra kết quả tổng hợp cuối cùng.

### 2. Khởi chạy tiến trình
Hệ thống thực thi tiến trình Hadoop Streaming thông qua gói thư viện java `hadoop-streaming-3.5.0.jar`, trỏ trực tiếp các tệp tin script python vào các tham số đầu vào và lưu trữ kết quả tại thư mục `/user/student/transactions/output` trên HDFS.

---

## IV. KẾT QUẢ CUỐI CÙNG & TẢI VỀ CỤC BỘ

Kết quả xử lý từ tệp kết quả `part-00000` được tải về thư mục cục bộ của Colab `/content/result_transactions.txt` thông qua lệnh:  
`hdfs dfs -get /user/student/transactions/output/part-00000 /content/result_transactions.txt`

