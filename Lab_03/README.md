# DS200_TH03 - Thực Hành 3: Spark RDD với Java

Repository này chứa mã nguồn giải bài tập Thực hành 3 (Lab 03) môn Big Data (DS200), sử dụng **Java** và **Apache Spark RDD** để xử lý tập dữ liệu đánh giá phim (Movie Lens Data).

## Thông tin sinh viên

- **Họ và tên:** Nguyễn Hải Đăng
- **MSSV:** 23520228
- **Môn:** DS200 - Phân tích dữ liệu lớn

---

## 📂 Cấu trúc thư mục

Dự án được tổ chức theo cấu trúc tiêu chuẩn của Maven như sau:

```text
Baithuchanh3/
├── pom.xml                          # Cấu hình dự án Maven & các dependency Hadoop MapReduce
├── run_all.sh                       # Bash script tự động build (.jar) và chạy tuần tự 6 task
├── README.md                        # Tài liệu hướng dẫn
├── assignments.ipynb                # File yêu cầu bài tập (Jupyter Notebook) gốc
├── src/main/java/                   # Mã nguồn Java (chia theo từng Task)
│   ├── Bai1.java                    # Task 1: Tính điểm đánh giá trung bình mỗi bộ phim và tìm phim Top 1
│   ├── Bai2.java                    # Task 2: Phân tích đánh giá theo Thể loại (Genre)
│   ├── Bai3.java                    # Task 3: Phân tích đánh giá theo Giới tính (Gender) cho mỗi phim
│   ├── Bai4.java                    # Task 4: Phân tích đánh giá theo Nhóm tuổi (Age Group)
│   ├── Bai5.java                    # Task 5: Phân tích đánh giá dựa trên Nghề nghiệp (Occupation)
│   ├── Bai6.java                    # Task 6: Phân tích điểm đánh giá trung bình theo từng Năm
│   └── RunAll.java                  # Chạy tuần tự cả 6 task
├── input/                           # Dataset đầu vào
│   ├── movies.txt                   # Thông tin phim (MovieID, Title, Genres)
│   ├── ratings_1.txt                # Đánh giá phim (Phần 1)
│   ├── ratings_2.txt                # Đánh giá phim (Phần 2)
│   ├── users.txt                    # Thông tin người dùng (Tuổi, Giới tính, Nghề nghiệp...)
│   └── occupation.txt               # ID và Tên nghề nghiệp
└── output/                          # Thư mục chứa kết quả xuất ra sau khi chạy
    ├── bai1/
    ├── bai2/
    ├── bai3/
    ├── bai4/
    ├── bai5/
    └── bai6/
```

---

## 🚀 Hướng dẫn chạy dự án

### Yêu cầu hệ thống

1. Java JDK 11
2. Apache Maven
3. Apache Hadoop (cần lệnh `hadoop` trong PATH)

Kiểm tra nhanh:

```bash
java -version
mvn -version
hadoop version
```

### Cách 1: Chạy tự động tất cả các Task

Script `run_all.sh` tự động biên dịch code Java bằng Maven và chạy lần lượt cả 6 bài. Đứng tại thư mục dự án và chạy:

```bash
cd Baithuchanh3
chmod +x run_all.sh
./run_all.sh
```

Hoặc dùng class `RunAll`:

```bash
cd Baithuchanh3
mvn clean package
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar RunAll .
```

### Cách 2: Chạy thủ công từng Task

Nếu bạn muốn tự chạy từng bước một (để kiểm tra kết quả lẻ):

> **Lưu ý khi copy lệnh từ README:**
> - Chỉ copy **dòng lệnh bên trong** khối code, **không** copy dòng ` ```bash ` hay ` ``` `.
> - Đường dẫn ratings phải **bọc trong dấu ngoặc kép** `"input/ratings_*.txt"`. Nếu không, zsh/bash sẽ tách thành 2 file riêng và báo lỗi `Usage: BaiX ...`.

**Bước 1: Di chuyển vào thư mục dự án, biên dịch file `.jar`**

```bash
cd Baithuchanh3
mvn clean package
```

**Bước 2: Khai báo biến môi trường (chạy một lần trong terminal)**

```bash
JAR=target/Baithuchanh3-1.0-SNAPSHOT.jar
INPUT=input
OUTPUT=output
RATINGS="${INPUT}/ratings_*.txt"
```

**Bước 3: Chạy từng task bằng `hadoop jar`**

*Task 1 — Điểm trung bình và tổng lượt đánh giá theo phim:*

```bash
hadoop jar "$JAR" Bai1 "$RATINGS" "$OUTPUT/bai1" "$INPUT/movies.txt"
```

*Task 2 — Phân tích theo thể loại:*

```bash
hadoop jar "$JAR" Bai2 "$RATINGS" "$OUTPUT/bai2" "$INPUT/movies.txt"
```

*Task 3 — Phân tích theo giới tính:*

```bash
hadoop jar "$JAR" Bai3 "$RATINGS" "$OUTPUT/bai3" "$INPUT/users.txt" "$INPUT/movies.txt"
```

*Task 4 — Phân tích theo nhóm tuổi:*

```bash
hadoop jar "$JAR" Bai4 "$RATINGS" "$OUTPUT/bai4" "$INPUT/users.txt" "$INPUT/movies.txt"
```

*Task 5 — Phân tích theo nghề nghiệp:*

```bash
hadoop jar "$JAR" Bai5 "$RATINGS" "$OUTPUT/bai5" "$INPUT/users.txt" "$INPUT/occupation.txt"
```

*Task 6 — Phân tích theo năm:*

```bash
hadoop jar "$JAR" Bai6 "$RATINGS" "$OUTPUT/bai6"
```

**Hoặc chạy trực tiếp không cần biến (copy từng dòng):**

```bash
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar Bai1 "input/ratings_*.txt" output/bai1 input/movies.txt
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar Bai2 "input/ratings_*.txt" output/bai2 input/movies.txt
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar Bai3 "input/ratings_*.txt" output/bai3 input/users.txt input/movies.txt
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar Bai4 "input/ratings_*.txt" output/bai4 input/users.txt input/movies.txt
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar Bai5 "input/ratings_*.txt" output/bai5 input/users.txt input/occupation.txt
hadoop jar target/Baithuchanh3-1.0-SNAPSHOT.jar Bai6 "input/ratings_*.txt" output/bai6
```

---

## 📊 Kết quả (Output)

Tất cả kết quả xử lý dữ liệu sẽ được tự động ghi vào các thư mục tương ứng bên trong folder `output/`. Ví dụ:

- `output/bai1/part-r-00000`
- `output/bai2/part-r-00000`
- `output/bai3/part-r-00000`

(Mỗi output bao gồm file `_SUCCESS` và file kết quả `part-r-00000`.)

---

## Xem kết quả output

Mỗi bài ghi kết quả vào thư mục `output/baiX/` dưới dạng MapReduce (`part-r-00000`):

```bash
cat output/bai1/part-r-00000
cat output/bai2/part-r-00000
cat output/bai3/part-r-00000
cat output/bai4/part-r-00000
cat output/bai5/part-r-00000
cat output/bai6/part-r-00000
```

Hoặc xem tất cả:

```bash
for i in 1 2 3 4 5 6; do
  echo "========== BAI $i =========="
  cat output/bai$i/part-r-00000
  echo
done
```

---

## Mô tả các bài

| Bài | Mục tiêu | Input chính |
|-----|----------|-------------|
| 1 | Điểm TB, tổng lượt rating, phim rating cao nhất (≥ 5 lượt) | ratings, movies |
| 2 | Điểm TB theo thể loại phim | ratings, movies |
| 3 | Điểm TB mỗi phim theo giới tính (M/F) | ratings, users, movies |
| 4 | Điểm TB mỗi phim theo nhóm tuổi | ratings, users, movies |
| 5 | Điểm TB và tổng lượt rating theo nghề nghiệp | ratings, users, occupation |
| 6 | Tổng lượt rating và điểm TB theo năm | ratings |

> **Lưu ý Bài 1:** Đề ghi ngưỡng 50 lượt đánh giá ở mục tiêu, nhưng bước giải pháp và dữ liệu mẫu dùng ngưỡng **≥ 5 lượt**. Code hiện tại dùng ngưỡng 5 (có thể đổi hằng số `MIN_RATINGS` trong `Bai1.java`).

---

## Ghi chú kỹ thuật

- Chạy ở chế độ **local** (không cần Hadoop cluster).
- Input ratings dùng glob `"input/ratings_*.txt"` (có dấu ngoặc kép) để Hadoop đọc cả `ratings_1.txt` và `ratings_2.txt` trong **một** tham số. Không dùng `input/ratings_*.txt` không quote — zsh sẽ expand thành 2 file và gây lỗi `Usage: BaiX ...`.
- Trước mỗi lần chạy, thư mục output tương ứng sẽ được xóa tự động nếu đã tồn tại.
- Parse `movies.txt` bằng `indexOf` / `lastIndexOf` vì Title có thể chứa dấu phẩy.

---

## Chụp màn hình nộp bài

1. Chạy `./run_all.sh` hoặc từng lệnh `hadoop jar` ở trên.
2. Chụp terminal khi job chạy thành công (`Job job_... completed successfully`).
3. Chụp nội dung `output/baiX/part-r-00000` cho từng bài.
