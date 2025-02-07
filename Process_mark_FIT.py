import os
import sys
import pandas as pd
import re
from unidecode import unidecode
import warnings
from pathlib import Path

# Ẩn cảnh báo
warnings.filterwarnings('ignore')


# Đường dẫn thư mục nguồn và đích
folder_path = Path(r"D:\Visual Studio Coode\Python\FIT_UDCNTT\DIEM THANH PHAN_KI 1, 2022_2023")
processed_path = Path(r"D:\Visual Studio Coode\Python\FIT_UDCNTT\Mark_data\Term1_2022_2023_processed")
teacher_file_path = Path(r"D:\Visual Studio Coode\Python\FIT_UDCNTT\TKB_GV khoa.xlsx")


# Các file output
merged_invalid_file_path = processed_path / 'INVALID_ALL.csv'
merged_file_path = processed_path / "Merged_Data.csv"
stats_file_path = processed_path / "Processing_Stats.csv"

# Tạo thư mục đích nếu chưa tồn tại
processed_path.mkdir(parents=True, exist_ok=True)

# DataFrame tổng hợp các bản ghi hợp lệ và không hợp lệ
merged_valid_df = pd.DataFrame()
merged_invalid_df = pd.DataFrame()

# Biến lưu thống kê
stats_list = []

def check_dup(dataframe):
    """Xóa các bản ghi trùng lặp theo cột 'Mã sinh viên' và hiển thị số dòng bị xóa"""
    # Lưu số dòng ban đầu
    initial_row_count = dataframe.shape[0]
    
    # Tìm các dòng trùng lặp (giữ lại lần đầu tiên xuất hiện)
    duplicates = dataframe[dataframe.duplicated(subset=['Mã sinh viên'], keep=False)]
    
    # In ra các dòng trùng lặp
    if not duplicates.empty:
        print("Các dòng trùng lặp:")
        print(duplicates[['Mã sinh viên', 'Chuyên cần', 'Giữa kì', 'Cuối kì']])  # Chọn các cột muốn hiển thị
    
    # Xóa các dòng trùng lặp theo cột 'Mã sinh viên'
    dataframe = dataframe.drop_duplicates(subset=['Mã sinh viên'])
    
    # Lưu số dòng sau khi xóa
    final_row_count = dataframe.shape[0]
    
    # Tính số dòng bị xóa
    rows_removed = initial_row_count - final_row_count
    
    print(f"\nSố dòng bị xóa do trùng lặp: {rows_removed}\n")
    
    return dataframe


def Remove_student(dataframe):
    """Xóa sinh viên có điểm không hợp lệ (không phải số), vắng thi (VT), hoặc cấm thi (CT)"""
    try:
        list_mark = ['Chuyên cần', 'Giữa kì', 'Cuối kì']
        
        # Lưu số dòng bị xóa
        rows_removed = 0
        
        for diem in list_mark:
            if diem in dataframe.columns:
                # Loại bỏ các dòng có điểm "VT" (vắng thi) hoặc "CT" (cấm thi)
                initial_count = len(dataframe)  # Đếm số dòng trước khi lọc
                dataframe = dataframe[~dataframe[diem].isin(['VT', 'CT'])]
                rows_removed += initial_count - len(dataframe)  # Tính số dòng bị xóa

                # Chuyển các giá trị không phải số thành NaN
                dataframe.loc[:, diem] = pd.to_numeric(dataframe[diem], errors='coerce')

        
        # Loại bỏ các dòng có giá trị NaN trong các cột điểm
        dataframe = dataframe.dropna(subset=list_mark)
        
        print(f"Đã xóa {rows_removed} dòng sinh viên không đủ điều kiện thi.\n")
        
    except Exception as e:
        print(f'Lỗi khi đọc cột: {e}')
        
    return dataframe


def format_scores(dataframe):
    """Định dạng các cột điểm"""
    try:
        list_mark = ['Chuyên cần', 'Giữa kì', 'Cuối kì']
        
        for diem in list_mark:
            if diem in dataframe.columns:
                # Thay dấu phẩy thành dấu chấm
                dataframe[diem] = dataframe[diem].astype(str).str.replace(',', '.')
                # Chuyển đổi sang kiểu float
                dataframe[diem] = pd.to_numeric(dataframe[diem], errors='coerce')
            else:
                print(f"Cảnh báo: Cột '{diem}' không tồn tại trong DataFrame.")
    except Exception as e:
        print(f"Lỗi khi định dạng cột điểm: {e}")
    
    return dataframe

def Classify_performace(score):
    """Phân loại học lực dựa trên thang điểm trung bình"""
    if 8.45 <= score <= 10:
        return 'Xuất sắc'
    elif 7.0 <= score <= 8.44:
        return 'Giỏi'
    elif 5.45 <= score <= 6.95:
        return 'Khá'
    elif 3.95 <= score <= 5.44:
        return 'Trung bình'
    else:
        return 'Yếu'

def format_teacher_name(full_name: str) -> str:
    """Chuẩn hóa tên giáo viên theo định dạng Tên + Chữ cái đầu họ đệm"""
    if pd.isna(full_name) or full_name.upper() == "TUANVM":
        return None
    clean_name = unidecode(str(full_name)).upper()
    parts = clean_name.split()
    if len(parts) > 1:
        first_name = parts[-1]
        initials = ''.join([p[0] for p in parts[:-1]])
        return f"{first_name}{initials}"
    return clean_name

def extract_group_number(df, sheet_name):
    """Trích xuất số nhóm từ nội dung sheet hoặc tên sheet"""
    pattern = re.compile(r'Nhóm\s*[:\-]?\s*.*?(\d+)', re.IGNORECASE)
    
    for col in df.columns:
        for cell in df[col].astype(str).dropna():
            cleaned_cell = ' '.join(cell.split())
            match = pattern.search(cleaned_cell)
            if match:
                return match.group(1)
    
    sheet_match = pattern.search(sheet_name)
    if sheet_match:
        return sheet_match.group(1)
    
    fallback_match = re.search(r'(\d+)', sheet_name)
    return fallback_match.group(1) if fallback_match else 'unknown'

def extract_term_year(df):
    """Trích xuất kỳ học và năm học từ dữ liệu"""
    pattern = re.compile(r'Học kì\s*(\d+).*?năm học\s*(\d{4}-\d{4})', re.IGNORECASE)
    
    for col in df.columns:
        for cell in df[col].astype(str).dropna():
            cleaned_cell = ' '.join(cell.split())
            match = pattern.search(cleaned_cell)
            if match:
                return match.groups()
    
    return 'unknown', 'unknown'

def clean_notes_column(df):
    """Làm sạch cột 'Ghi chú'"""
    if 'Ghi chú' in df.columns:
        df['Ghi chú'] = df['Ghi chú'].str.replace(r'^\s*\|\s*', '', regex=True)
    return df

def process_student_data(raw_df, sheet_name, file_name):
    """Xử lý dữ liệu sinh viên từ một sheet"""
    try:
        header_idx = raw_df[raw_df.iloc[:, 0].astype(str).str.strip() == 'STT'].index
        if header_idx.empty:
            raise ValueError("Không tìm thấy header chứa STT")
        
        header_idx = header_idx[0]
        raw_df.columns = [re.sub(r'\s+', ' ', str(col).strip()) for col in raw_df.iloc[header_idx]]
        df = raw_df.iloc[header_idx + 1:].reset_index(drop=True)

        column_mapping = {
            r'Mã sinh viên|MSSV': 'Mã sinh viên',
            r'Điểm chuyên.*|Điểm CC.*': 'Chuyên cần',
            r'Điểm giữa.*|Điểm GK.*|Điêm GK.*|Điểm thường xuyên.*': 'Giữa kì',
            r'Điểm cuối.*': 'Cuối kì',
            r'Ghi chú': 'Ghi chú'
        }

        for pattern, new_name in column_mapping.items():
            matches = df.columns.str.contains(pattern, regex=True)
            if any(matches):
                df.rename(columns={df.columns[matches][0]: new_name}, inplace=True)

        required_cols = ['Mã sinh viên', 'Chuyên cần', 'Giữa kì', 'Cuối kì']
        df = df[required_cols].copy()

        df = df[df['Mã sinh viên'].notna() & df['Mã sinh viên'].str.strip().ne('')]

        numeric_cols = ['Chuyên cần', 'Giữa kì', 'Cuối kì']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        invalid_df = df[df[numeric_cols].isna().any(axis=1)].copy()
        valid_df = df.dropna(subset=numeric_cols).copy()

        invalid_df = clean_notes_column(invalid_df)
        
        nhóm_value = extract_group_number(raw_df, sheet_name)
        ky_hoc, nam_hoc = extract_term_year(raw_df)
        
        valid_df['Học kì'] = ky_hoc
        valid_df['Năm học'] = nam_hoc
        valid_df['Nhóm'] = nhóm_value
        invalid_df['Nhóm'] = nhóm_value


        major_dict = {
            "701": "Ngôn ngữ Anh", "702": "Ngôn ngữ Nga", "703": "Ngôn ngữ Pháp",
            "704": "Ngôn ngữ Trung", "714": "Ngôn ngữ Trung - CLC", "705": "Ngôn ngữ Đức",
            "706": "Ngôn ngữ Nhật Bản", "707": "Ngôn ngữ Hàn Quốc", "717": "Ngôn ngữ Hàn Quốc - CLC",
            "708": "Ngôn ngữ Tây Ban Nha", "709": "Ngôn ngữ Italia", "719": "Ngôn ngữ Italia - CLC",
            "710": "Ngôn ngữ Bồ Đào Nha", "104": "Công nghệ thông tin", "114": "Công nghệ thông tin - CLC",
            "400": "Quản trị kinh doanh", "405": "Marketing", "401": "Kế Toán", "404": "Tài chính Ngân hàng",
            "608": "Quốc tế học", "606": "Nghiên cứu phát triển", "609": "Quản trị dịch vụ du lịch và lữ hành",
            "619": "Quản trị dịch vụ du lịch và lữ hành - CLC", "100": "Truyền thông doanh nghiệp",
            "106": "Truyền thông đa phương tiện"
        }
        
        for df_part in [valid_df, invalid_df]:
            df_part['Chuyên ngành'] = df['Mã sinh viên'].str[3:6].map(major_dict).fillna('Quốc tế học')

        if not valid_df.empty:
            valid_df = format_scores(valid_df)
            valid_df['TB chung môn học'] = (valid_df['Giữa kì'] * 0.3) + (valid_df['Cuối kì'] * 0.6) + (valid_df['Chuyên cần'] * 0.1)
            valid_df['TB chung môn học'] = pd.to_numeric(valid_df['TB chung môn học'], errors='coerce').round(2)
            valid_df['Xếp loại'] = valid_df['TB chung môn học'].apply(Classify_performace)
            valid_df['Kết quả'] = valid_df['TB chung môn học'].apply(lambda x: 'Đỗ' if x >= 4.95 else 'Trượt')
            valid_df = check_dup(valid_df)
            valid_df = Remove_student(valid_df)
            valid_df = format_scores(valid_df)
        
        return valid_df, invalid_df
        
    except Exception as e:
        print(f'⚠️ Lỗi khi xử lý dữ liệu sheet {file_name}: {str(e)}')
        sys.exit(1)

def add_to_merged_invalid(merged_df, new_invalid_df):
    """Thêm dữ liệu không hợp lệ vào DataFrame tổng hợp"""
    if not merged_df.empty:
        merged_df = pd.concat([merged_df, new_invalid_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=['Mã sinh viên', 'Nhóm'])
    else:
        merged_df = new_invalid_df.copy()
    return merged_df

# Xử lý dữ liệu học sinh
total_files = len([f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls')) and not f.startswith('~$')])
processed_files = 0

print(f'📂 Tổng số file trong thư mục: {total_files}\n')

for file_name in os.listdir(folder_path):
    if file_name.endswith(('.xlsx', '.xls')) and not file_name.startswith('~$'):
        file_path = folder_path / file_name
        print(f'🚀 Đang xử lý file: {file_name}...')

        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_count = len(excel_file.sheet_names)
            processed_sheets = []
            
            for sheet_name in excel_file.sheet_names:
                print(f' ➤ Đọc sheet: {sheet_name}...')

                df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
                valid_df, invalid_df = process_student_data(df, sheet_name, file_name)

                valid_count = len(valid_df)
                invalid_count = len(invalid_df)

                if valid_count > 0:
                    merged_valid_df = pd.concat([merged_valid_df, valid_df], ignore_index=True)
                
                if invalid_count > 0:
                    invalid_df['File'] = file_name
                    invalid_df['Sheet'] = sheet_name
                    merged_invalid_df = add_to_merged_invalid(merged_invalid_df, invalid_df)

                print(f' ✅ Đã xử lý: {sheet_name}| Hợp lệ: {valid_count} | Không hợp lệ: {invalid_count}\n')
                processed_sheets.append(sheet_name)

            stats_list.append({
                'File': file_name,
                'Số sheet': sheet_count,
                'Sheet đã xử lý': ', '.join(processed_sheets)
            })
            processed_files += 1

        except Exception as e:
            print(f'❌ Lỗi khi xử lý file {file_name}: {str(e)}')

# Xử lý dữ liệu giáo viên và merge
try:
    df_teacher = pd.read_excel(
        teacher_file_path,
        sheet_name="Thong ke gio day HK1",
        skiprows=2,
        usecols="A:G",
        dtype={'Lớp': 'string', 'Giáo viên': 'string'}
    )
    
    df_teacher = df_teacher.dropna(how='all').reset_index(drop=True)
    df_teacher['Lớp'] = df_teacher['Lớp'].str.replace('UDCNTT_', '', regex=False)
    df_teacher['Giảng viên'] = df_teacher['Giáo viên'].apply(format_teacher_name)
    
    final_teacher_df = df_teacher[["Giảng viên", "Lớp"]].dropna(subset=['Giảng viên', 'Lớp'])
    
    # Merge vào dữ liệu học sinh
    merged_valid_df = pd.merge(
        merged_valid_df,
        final_teacher_df,
        left_on='Nhóm',
        right_on='Lớp',
        how='left'
    ).drop('Lớp', axis=1, errors='ignore')

except Exception as e:
    print(f"❌ Lỗi khi xử lý file giảng viên: {str(e)}")
    sys.exit(1)

# Lưu các file output
if not merged_valid_df.empty:
    merged_valid_df = merged_valid_df.drop_duplicates(subset=['Mã sinh viên', 'Nhóm'], keep='first')
    merged_valid_df = merged_valid_df.sort_values(by=['Nhóm', 'Mã sinh viên'], key=lambda x: pd.to_numeric(x, errors='coerce'))
    merged_valid_df.to_csv(merged_file_path, index=False, encoding='utf-8-sig')
    print(f'📁 Đã lưu file tổng hợp: {merged_file_path}')

if not merged_invalid_df.empty:
    merged_invalid_df.to_csv(merged_invalid_file_path, index=False, encoding='utf-8-sig')
    print(f'📁 Đã lưu bản ghi không hợp lệ: {merged_invalid_file_path}')

stats_df = pd.DataFrame(stats_list)
if not stats_df.empty:
    stats_df.to_csv(stats_file_path, index=False, encoding='utf-8-sig')
    print(f'📊 Đã lưu thống kê xử lý: {stats_file_path}')

print("\n✅ Hoàn thành xử lý tất cả dữ liệu!")
