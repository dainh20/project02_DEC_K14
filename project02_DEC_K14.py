# project02_DEC_K14
import requests
import pandas as pd
import json
import time
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import RequestException

# Đọc danh sách product_id từ file CSV
product_ids = pd.read_csv('/home/dai/Documents/project2/products-0-200000(in).csv')['id'].tolist()

# API endpoint để lấy chi tiết sản phẩm
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/"

# Hàm chuẩn hóa nội dung mô tả (description)
def clean_description(description):
    # Tiến hành chuẩn hóa nội dung mô tả như xóa khoảng trắng thừa hoặc các ký tự đặc biệt
    description = description.strip()
    return description

# Hàm tải chi tiết sản phẩm từ API
def get_product_details(product_id):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }        
        response = requests.get(f"{API_URL}{product_id}", headers=headers)
        response.raise_for_status()  # Kiểm tra nếu có lỗi HTTP
        product_data = response.json()
        
        # Lấy thông tin cần thiết
        product_info = {
            "id": product_data.get("id", ""),
            "name": product_data.get("name", ""),
            "url_key": product_data.get("url_key", ""),
            "price": product_data.get("price", ""),
            "description": clean_description(product_data.get("description", "")),
            "images": product_data.get("images", [])
        }
        return product_info
    except RequestException as e:
        print(f"Error fetching product {product_id}: {e}")
        return None

# Hàm ghi dữ liệu sản phẩm vào file JSON
def save_products_to_file(products, file_index):
    file_name = f"/home/dai/Documents/project2/result/products_{file_index}.json"
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=4)
    print(f"Saved {len(products)} products to {file_name}")

# Hàm ghi các id lỗi vào file text
def save_error_ids(error_ids):
    error_file = "/home/dai/Documents/project2/err_id/error_ids.txt"
    with open(error_file, 'w', encoding='utf-8') as f:
        for error_id in error_ids:
            f.write(f"{error_id}\n")
    print(f"Saved {len(error_ids)} error IDs to {error_file}")

# Hàm chính để tải và lưu thông tin sản phẩm
def fetch_and_save_products():
    products = []
    error_ids = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for i, product_id in enumerate(product_ids):
            futures.append(executor.submit(get_product_details, product_id))

            # Khi đã có đủ 1000 sản phẩm, lưu vào file
            if len(futures) == 1000:
                completed = [future.result() for future in futures]
                valid_products = [p for p in completed if p]  # Lọc các sản phẩm không có dữ liệu
                products.extend(valid_products)
                error_ids.extend([product_ids[j] for j in range(i-999, i+1) if not completed[j-i+999]])  # Lưu các id lỗi
                save_products_to_file(products, i // 1000)
                products = []  # Reset list for next 1000
                futures = []  # Reset futures list

        # Lưu phần còn lại nếu có
        if len(futures) > 0:
            completed = [future.result() for future in futures]
            valid_products = [p for p in completed if p]
            products.extend(valid_products)
            error_ids.extend([product_ids[j] for j in range(i-len(futures)+1, i+1) if not completed[j-i+len(futures)-1]]) 
            save_products_to_file(products, len(product_ids) // 1000)

    # Lưu các id lỗi dưới dạng text
    save_error_ids(error_ids)
    print(f"All products fetched and saved in {time.time() - start_time} seconds.")

# Chạy chương trình
fetch_and_save_products()

#Saved 1059 error IDs to /home/dai/Documents/project2/err_id/error_ids.txt
#All products fetched and saved in 5892.458546876907 seconds.



