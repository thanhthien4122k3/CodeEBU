# UDCNTT_HANU
---

# **Project: Xử Lý và Tổng Hợp Dữ Liệu Điểm Sinh Viên**

## Mô tả

Project này là một script Python được thiết kế để tự động hóa quy trình xử lý và tổng hợp dữ liệu điểm thi của sinh viên từ các file Excel. Script thực hiện các bước sau:

1.  **Đọc dữ liệu:** Tự động quét và đọc dữ liệu điểm từ nhiều file Excel nằm trong một thư mục chỉ định.
2.  **Chuẩn hóa dữ liệu:**
    *   Loại bỏ các dòng dữ liệu bị trùng lặp dựa trên mã sinh viên.
    *   Xóa bỏ những sinh viên có điểm không hợp lệ (ví dụ: điểm không phải số, hoặc các ký tự như 'VT' - vắng thi, 'CT' - cấm thi).
    *   Định dạng chuẩn các cột điểm (chuyển đổi dấu phẩy thành dấu chấm và đảm bảo định dạng số).
3.  **Phân loại học lực:** Tự động phân loại học lực của sinh viên dựa trên điểm trung bình môn học theo các mức độ: Xuất sắc, Giỏi, Khá, Trung bình, Yếu.
4.  **Tổng hợp dữ liệu:**
    *   Gộp dữ liệu từ nhiều file và sheet khác nhau thành một file duy nhất.
    *   Tạo file riêng biệt chứa các bản ghi không hợp lệ để dễ dàng kiểm tra và đối chiếu.
5.  **Thống kê quá trình xử lý:** Xuất ra file thống kê chi tiết về số lượng file và sheet đã được xử lý, giúp người dùng dễ dàng theo dõi và kiểm soát quá trình.
6.  **Kết hợp dữ liệu giáo viên:** Tự động kết hợp thông tin về giáo viên giảng dạy từ một file Excel khác, giúp liên kết dữ liệu điểm với thông tin giáo viên.

## Chức năng chính

*   **Tự động hóa xử lý dữ liệu:** Giảm thiểu công sức xử lý thủ công các file điểm số lượng lớn.
*   **Làm sạch và chuẩn hóa dữ liệu:** Đảm bảo dữ liệu đầu vào được sạch sẽ và ở định dạng chuẩn, sẵn sàng cho các phân tích tiếp theo.
*   **Phân loại học lực tự động:**  Nhanh chóng có được cái nhìn tổng quan về phân bố học lực của sinh viên.
*   **Tổng hợp dữ liệu đa nguồn:**  Dễ dàng tập hợp dữ liệu từ nhiều nguồn khác nhau vào một nơi duy nhất.
*   **Cung cấp thông tin thống kê:**  Giúp người dùng nắm bắt được tình hình xử lý dữ liệu và chất lượng dữ liệu đầu vào.
*   **Kết nối thông tin giáo viên:**  Mở rộng khả năng phân tích dữ liệu bằng cách tích hợp thông tin về giáo viên.

## Nội dung các file
* 📄 TKB_GV.xlsx (File TKB giáo viên)
```
 Từ cột A:G **phải có 3 cột** (STT, Lớp và Giáo viên) và tên là "Thong ke gio day HK1"
   ![image](https://github.com/user-attachments/assets/35b39bb5-2b11-4e45-b5f0-92428c360d66)
```
*📄 File điểm 1.xlsx
```
Bắt buộc
+ Phải có dòng: 	Học kì 1. năm học 2022-2023		Môn: Ứng dụng CNTT 				NHÓM 01 

+ Phải có các cột: STT, "Điểm CC (10%)", "Điểm GK (30%)", "Điểm cuối kỳ (60%)", MSSV, Ghi chú (Lưu ý: có thể không viết tắt cũng được)

![image](https://github.com/user-attachments/assets/e5e99a33-9f52-4861-b5a1-963ea43410da)

```

## Yêu cầu

Trước khi sử dụng script này, bạn cần đảm bảo đã cài đặt các thư viện Python sau:

```bash
pip install pandas unidecode openpyxl
```

*   **pandas**: Thư viện mạnh mẽ để phân tích và thao tác dữ liệu.
*   **unidecode**:  Để xử lý và chuẩn hóa tên giáo viên, loại bỏ dấu tiếng Việt.
*   **openpyxl**: Thư viện để đọc và viết file Excel (đặc biệt là `.xlsx`). Python có thể đã cài đặt sẵn thư viện này, nhưng nếu gặp lỗi bạn có thể cài đặt thủ công.

Ngoài ra, bạn cần có các file dữ liệu đầu vào theo cấu trúc thư mục như mô tả dưới đây.

## Hướng dẫn sử dụng

### 1. Cấu trúc thư mục

Trước khi chạy script, hãy chắc chắn rằng bạn đã thiết lập cấu trúc thư mục như sau:

```
📁 Tên thư mục gốc (ví dụ: Mark_data)
├── 📁 DIEM THANH PHAN_KI 1, 2022_2023 (Thư mục chứa file điểm gốc)
│   ├── 📄 File điểm 1.xlsx
│   ├── 📄 File điểm 2.xls
│   └── 📄 ... (Các file điểm khác)
├── 📁 Term1_2022_2023_processed (Thư mục để lưu file đã xử lý)
│   └── (Thư mục này sẽ được tạo tự động nếu chưa tồn tại)
└── 📄 TKB_GV.xlsx (File TKB giáo viên)
```

*   **`DIEM THANH PHAN_KI 1, 2022_2023`**: Thư mục chứa tất cả các file Excel điểm thành phần của sinh viên ở định dạng `.xlsx` hoặc `.xls`.
*   **`Term1_2022_2023_processed`**: Thư mục này sẽ chứa các file kết quả sau khi script chạy xong. Nếu thư mục này chưa tồn tại, script sẽ tự động tạo mới.
*   **`TKB_GV.xlsx`**: File Excel chứa thông tin về thời khóa biểu và giáo viên, sử dụng để ghép thông tin giáo viên vào dữ liệu điểm.

**Lưu ý quan trọng:**

*   Đảm bảo rằng tên các thư mục và file trong script trùng khớp với tên thực tế trên máy của bạn, hoặc bạn cần chỉnh sửa lại các đường dẫn trong code cho phù hợp.
*   Các file Excel điểm sinh viên cần có cấu trúc tương tự nhau, với header chứa các thông tin như 'STT', 'Mã sinh viên', 'Điểm chuyên cần', 'Điểm giữa kì', 'Điểm cuối kì', 'Ghi chú'. Script có khả năng tự động nhận diện các cột này dù tên cột có thể khác nhau một chút (ví dụ: 'MSSV' thay vì 'Mã sinh viên').

### 2. Chỉnh sửa đường dẫn (nếu cần)

Mở file Python và kiểm tra các đường dẫn sau, điều chỉnh nếu cần thiết để phù hợp với vị trí thư mục và file trên máy tính của bạn:

```python
# Đường dẫn thư mục nguồn và đích
folder_path = Path(r"D:\Visual Studio Coode\Python\FIT_UDCNTT\DIEM THANH PHAN_KI 1, 2022_2023")
processed_path = Path(r"D:\Visual Studio Coode\Python\FIT_UDCNTT\Mark_data\Term1_2022_2023_processed")
teacher_file_path = Path(r"D:\Visual Studio Coode\Python\FIT_UDCNTT\TKB_GV khoa.xlsx")
```

### 3. Chạy script

Mở terminal hoặc command prompt, điều hướng đến thư mục chứa file Python script và chạy lệnh:

```bash
python tên_file_script.py
```

Thay `tên_file_script.py` bằng tên file Python của bạn.

### 4. Kiểm tra kết quả

Sau khi script chạy thành công, bạn sẽ tìm thấy các file kết quả trong thư mục `Term1_2022_2023_processed`:

*   **`Merged_Data.csv`**: File CSV chứa dữ liệu điểm đã được tổng hợp, làm sạch và chuẩn hóa của tất cả sinh viên hợp lệ từ các file đầu vào. File này đã bao gồm thông tin về chuyên ngành, điểm trung bình môn học, xếp loại học lực, kết quả (Đỗ/Trượt) và thông tin giáo viên (nếu có).
*   **`INVALID_ALL.csv`**: File CSV chứa tất cả các bản ghi điểm không hợp lệ từ các file đầu vào. Các bản ghi này có thể chứa lỗi định dạng điểm, thiếu thông tin điểm, hoặc các lỗi khác. File này giúp bạn dễ dàng kiểm tra và đối chiếu các trường hợp dữ liệu không hợp lệ.
*   **`Processing_Stats.csv`**: File CSV chứa thống kê chi tiết về quá trình xử lý, bao gồm số lượng file đã xử lý, số lượng sheet trong mỗi file, và các sheet nào đã được xử lý. File này cung cấp cái nhìn tổng quan về quá trình chạy script và có thể giúp ích trong việc kiểm soát chất lượng dữ liệu đầu vào.

## Các hàm chính

Script này bao gồm các hàm chính sau:

*   **`check_dup(dataframe)`**: Xóa các bản ghi trùng lặp trong DataFrame dựa trên cột 'Mã sinh viên'.
*   **`Remove_student(dataframe)`**: Loại bỏ sinh viên có điểm không hợp lệ (không phải số, 'VT', 'CT').
*   **`format_scores(dataframe)`**: Định dạng các cột điểm số trong DataFrame.
*   **`Classify_performace(score)`**: Phân loại học lực dựa trên điểm trung bình.
*   **`format_teacher_name(full_name)`**: Chuẩn hóa tên giáo viên.
*   **`extract_group_number(df, sheet_name)`**: Trích xuất số nhóm từ sheet.
*   **`extract_term_year(df)`**: Trích xuất học kỳ và năm học.
*   **`clean_notes_column(df)`**: Làm sạch cột 'Ghi chú'.
*   **`process_student_data(raw_df, sheet_name, file_name)`**: Hàm chính xử lý dữ liệu từ một sheet, bao gồm chuẩn hóa, làm sạch, phân loại và tổng hợp dữ liệu.
*   **`add_to_merged_invalid(merged_df, new_invalid_df)`**:  Thêm dữ liệu không hợp lệ mới vào DataFrame tổng hợp các bản ghi không hợp lệ.

## Lưu ý

*   **Kiểm tra đường dẫn:** Luôn đảm bảo các đường dẫn file và thư mục trong script là chính xác.
*   **Cấu trúc file Excel đầu vào:** Script được thiết kế để hoạt động với cấu trúc file Excel nhất định. Nếu cấu trúc file của bạn khác biệt đáng kể, có thể cần chỉnh sửa script để phù hợp.
*   **File TKB giáo viên (teacher\_file\_path):**  File này là tùy chọn. Nếu bạn không có file này hoặc không muốn ghép thông tin giáo viên, bạn có thể bỏ qua bước này hoặc comment đoạn code liên quan đến xử lý file giáo viên. Script vẫn sẽ hoạt động bình thường để xử lý và tổng hợp dữ liệu điểm sinh viên.
*   **Backup dữ liệu gốc:**  Luôn nên backup dữ liệu gốc trước khi chạy script để tránh mất mát dữ liệu không mong muốn.
*   **Xử lý lỗi:** Script đã được thiết kế để xử lý một số lỗi thường gặp (ví dụ: lỗi đọc file, lỗi định dạng dữ liệu). Tuy nhiên, trong quá trình sử dụng, nếu gặp bất kỳ lỗi nào, hãy kiểm tra thông báo lỗi một cách cẩn thận. Nếu cần, bạn có thể liên hệ với tác giả script để được hỗ trợ thêm.

## Tác giả

[Vũ Thanh Thiên]

## Ngày

[06-02-2025]

---
