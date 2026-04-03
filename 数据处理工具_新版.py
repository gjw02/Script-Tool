import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import chardet
import re
import shutil
import time
import queue


class DataProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("数据处理工具 - 合并与分解")
        self.root.geometry("700x600")
        
        # 创建Notebook实现标签页
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 创建合并页面
        self.merge_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.merge_frame, text="文件合并")
        self.setup_merge_ui()
        
        # 创建分解页面
        self.split_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.split_frame, text="文件分解")
        self.setup_split_ui()
        
        # 创建上传监控页面
        self.upload_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.upload_frame, text="文件上传")
        self.setup_upload_ui()
        
        # 状态变量
        self.merging = False
        
    def setup_merge_ui(self):
        """设置合并功能界面"""
        # 输入文件夹选择
        ttk.Label(self.merge_frame, text="输入文件夹:").grid(row=0, column=0, sticky=tk.W, pady=5, padx=10)
        self.merge_input_path = tk.StringVar()
        ttk.Entry(self.merge_frame, textvariable=self.merge_input_path, width=50).grid(row=0, column=1, padx=5)
        ttk.Button(self.merge_frame, text="浏览...", command=self.browse_merge_input).grid(row=0, column=2, padx=5)
        
        # 输出文件选择
        ttk.Label(self.merge_frame, text="输出文件:").grid(row=1, column=0, sticky=tk.W, pady=5, padx=10)
        self.merge_output_path = tk.StringVar()
        ttk.Entry(self.merge_frame, textvariable=self.merge_output_path, width=50).grid(row=1, column=1, padx=5)
        ttk.Button(self.merge_frame, text="浏览...", command=self.browse_merge_output).grid(row=1, column=2, padx=5)
        
        # 编码选择
        ttk.Label(self.merge_frame, text="文件编码:").grid(row=2, column=0, sticky=tk.W, pady=5, padx=10)
        self.merge_encoding = tk.StringVar(value="自动检测")
        encoding_options = ["自动检测", "utf-8", "gbk", "gb2312", "latin1", "iso-8859-1"]
        encoding_menu = ttk.Combobox(self.merge_frame, textvariable=self.merge_encoding, values=encoding_options, width=15)
        encoding_menu.grid(row=2, column=1, sticky=tk.W)
        ttk.Label(self.merge_frame, text="(遇到编码错误时选择)").grid(row=2, column=1, sticky=tk.E, padx=5)
        
        # 合并按钮
        self.merge_btn = ttk.Button(self.merge_frame, text="合并文件", command=self.start_merge_thread)
        self.merge_btn.grid(row=3, column=1, pady=10)
        
        # 进度条
        self.merge_progress = ttk.Progressbar(self.merge_frame, orient=tk.HORIZONTAL, length=400, mode='determinate')
        self.merge_progress.grid(row=4, column=0, columnspan=3, pady=10, padx=10)
        
        # 日志区域
        merge_log_frame = ttk.LabelFrame(self.merge_frame, text="操作日志", padding=5)
        merge_log_frame.grid(row=5, column=0, columnspan=3, sticky="nsew", pady=5, padx=10)
        self.merge_frame.rowconfigure(5, weight=1)
        
        self.merge_log_text = tk.Text(merge_log_frame, height=8, width=70)
        self.merge_log_text.pack(fill=tk.BOTH, expand=True)
        merge_scrollbar = ttk.Scrollbar(merge_log_frame, command=self.merge_log_text.yview)
        merge_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.merge_log_text.config(yscrollcommand=merge_scrollbar.set)
        self.merge_log_text.config(state=tk.DISABLED)     
   
    def setup_split_ui(self):
        """设置分解功能界面"""
        # 文件选择框架
        file_frame = ttk.LabelFrame(self.split_frame, text="文件选择", padding=10)
        file_frame.pack(fill="x", padx=10, pady=5)
        
        # 输入文件
        ttk.Label(file_frame, text="数据文件:").grid(row=0, column=0, sticky="w")
        self.split_input_file = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.split_input_file, width=50).grid(row=0, column=1, padx=5)
        ttk.Button(file_frame, text="浏览", command=self.select_split_input).grid(row=0, column=2)
        
        # 输出目录
        ttk.Label(file_frame, text="输出目录:").grid(row=1, column=0, sticky="w")
        self.split_output_dir = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.split_output_dir, width=50).grid(row=1, column=1, padx=5)
        ttk.Button(file_frame, text="浏览", command=self.select_split_output).grid(row=1, column=2)
        
        # 文件信息框架
        info_frame = ttk.LabelFrame(self.split_frame, text="文件信息", padding=10)
        info_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(info_frame, text="检测编码:").grid(row=0, column=0, sticky="w")
        self.split_encoding = tk.StringVar()
        ttk.Label(info_frame, textvariable=self.split_encoding, foreground="blue").grid(row=0, column=1, sticky="w")
        
        # 条码列选择
        column_frame = ttk.LabelFrame(self.split_frame, text="列设置", padding=10)
        column_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(column_frame, text="将作为目录的列名:").grid(row=0, column=0, sticky="w")
        self.split_barcode_column = tk.StringVar()
        ttk.Entry(column_frame, textvariable=self.split_barcode_column, width=30).grid(row=0, column=1, padx=5)
        ttk.Label(column_frame, text="(留空将使用第一列)").grid(row=0, column=2, sticky="w")
        
        # 进度条
        progress_frame = ttk.LabelFrame(self.split_frame, text="进度", padding=10)
        progress_frame.pack(fill="x", padx=10, pady=5)
        
        self.split_progress = ttk.Progressbar(progress_frame, maximum=100)
        self.split_progress.pack(fill="x")
        
        self.split_status_label = ttk.Label(progress_frame, text="就绪")
        self.split_status_label.pack(pady=5)
        
        # 按钮
        button_frame = ttk.Frame(self.split_frame)
        button_frame.pack(pady=10)
        
        ttk.Button(button_frame, text="开始分解", command=self.start_splitting).pack(side="left", padx=5)
        
        # 日志区域
        split_log_frame = ttk.LabelFrame(self.split_frame, text="操作日志", padding=5)
        split_log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.split_log_text = tk.Text(split_log_frame, height=8, width=70)
        self.split_log_text.pack(fill=tk.BOTH, expand=True)
        split_scrollbar = ttk.Scrollbar(split_log_frame, command=self.split_log_text.yview)
        split_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.split_log_text.config(yscrollcommand=split_scrollbar.set)
        self.split_log_text.config(state=tk.DISABLED)
        
    def setup_upload_ui(self):
        """设置文件上传监控界面"""
        # 初始化上传相关变量
        self.upload_destination_path = ""
        self.upload_monitoring_folder = ""
        self.is_upload_monitoring = False
        self.upload_monitoring_thread = None
        self.uploaded_files_set = set()
        self.upload_history_file_path = ""
        self.upload_ui_queue = queue.Queue()
        
        # 源文件夹选择
        source_frame = ttk.Frame(self.upload_frame)
        source_frame.pack(fill=tk.X, pady=(10, 5), padx=10)
        
        self.select_source_button = ttk.Button(source_frame, text="选择监控文件夹", command=self.select_upload_folder)
        self.select_source_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.upload_folder_label = ttk.Label(source_frame, text="未选择文件夹", wraplength=450)
        self.upload_folder_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # 目标文件夹选择
        dest_frame = ttk.Frame(self.upload_frame)
        dest_frame.pack(fill=tk.X, pady=(5, 10), padx=10)
        
        self.select_dest_button = ttk.Button(dest_frame, text="选择上传位置", command=self.select_upload_destination)
        self.select_dest_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.upload_dest_label = ttk.Label(dest_frame, text="未选择上传位置", wraplength=450)
        self.upload_dest_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # 日志列表
        log_frame_label = ttk.Label(self.upload_frame, text="上传日志:")
        log_frame_label.pack(fill=tk.X, anchor=tk.W, padx=10)
        
        list_frame = ttk.Frame(self.upload_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5, padx=10)
        
        self.upload_log_listbox = tk.Listbox(list_frame)
        self.upload_log_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        upload_scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.upload_log_listbox.yview)
        upload_scrollbar.pack(side=tk.RIGHT, fill="y")
        self.upload_log_listbox.config(yscrollcommand=upload_scrollbar.set)
        
        # 控制按钮
        control_frame = ttk.Frame(self.upload_frame)
        control_frame.pack(fill=tk.X, pady=5, padx=10)
        
        self.upload_start_button = ttk.Button(control_frame, text="开始监控", command=self.start_upload_monitoring)
        self.upload_start_button.pack(side=tk.LEFT, padx=5, expand=True)
        
        self.upload_stop_button = ttk.Button(control_frame, text="停止监控", command=self.stop_upload_monitoring, state=tk.DISABLED)
        self.upload_stop_button.pack(side=tk.LEFT, padx=5, expand=True)
        
        # 状态和进度
        status_frame = ttk.Frame(self.upload_frame)
        status_frame.pack(fill=tk.X, pady=(5, 10), padx=10)
        
        self.upload_status_label = ttk.Label(status_frame, text="请选择监控文件夹和上传位置。", anchor=tk.W)
        self.upload_status_label.pack(fill=tk.X)
        
        self.upload_progress_bar = ttk.Progressbar(status_frame, orient="horizontal", length=100, mode="determinate")
        self.upload_progress_bar.pack(fill=tk.X, pady=5)
        
        # 启动队列处理
        self.root.after(100, self.process_upload_queue)    #
 
    def log_merge(self, message):
        """合并页面日志"""
        self.merge_log_text.config(state=tk.NORMAL)
        self.merge_log_text.insert(tk.END, message + "\n")
        self.merge_log_text.see(tk.END)
        self.merge_log_text.config(state=tk.DISABLED)
        
    def log_split(self, message):
        """分解页面日志"""
        self.split_log_text.config(state=tk.NORMAL)
        self.split_log_text.insert(tk.END, message + "\n")
        self.split_log_text.see(tk.END)
        self.split_log_text.config(state=tk.DISABLED)
    
    # 文件选择方法
    def browse_merge_input(self):
        folder = filedialog.askdirectory()
        if folder:
            self.merge_input_path.set(folder)
            self.log_merge(f"已选择输入文件夹: {folder}")
            
    def browse_merge_output(self):
        file_types = [("CSV 文件", "*.csv"), ("Excel 文件", "*.xlsx")]
        file = filedialog.asksaveasfilename(filetypes=file_types, defaultextension=".csv")
        if file:
            self.merge_output_path.set(file)
            self.log_merge(f"已选择输出文件: {file}")
            
    def select_split_input(self):
        filename = filedialog.askopenfilename(
            title="选择数据文件",
            filetypes=[
                ("Excel文件", "*.xlsx *.xls"),
                ("CSV文件", "*.csv"),
                ("所有文件", "*.*")
            ]
        )
        if filename:
            self.split_input_file.set(filename)
            if filename.lower().endswith('.csv'):
                threading.Thread(target=self.detect_split_encoding, args=(filename,)).start()
            else:
                self.split_encoding.set("Excel格式")
                
    def detect_split_encoding(self, filename):
        """检测分解文件的编码"""
        try:
            with open(filename, 'rb') as f:
                byte_data = f.read()
                
            try:
                byte_data.decode('utf-8')
                encoding = 'utf-8'
            except UnicodeDecodeError:
                result = chardet.detect(byte_data)
                encoding = result['encoding'] if result['encoding'] else 'gbk'
                
            self.root.after(0, lambda: self.split_encoding.set(encoding.upper()))
        except Exception:
            self.root.after(0, lambda: self.split_encoding.set("检测失败"))
            
    def select_split_output(self):
        directory = filedialog.askdirectory(title="选择输出目录")
        if directory:
            self.split_output_dir.set(directory)
    
    # 高级文件检测方法（从hb ec.py整合）
    def detect_file_encoding(self, file_path):
        """检测文件编码"""
        try:
            with open(file_path, 'rb') as f:
                rawdata = f.read(10000)
                result = chardet.detect(rawdata)
                encoding = result['encoding'] if result['confidence'] > 0.7 else 'utf-8'
                return encoding
        except Exception as e:
            self.log_merge(f"编码检测失败: {str(e)}")
            return 'utf-8'

    def detect_string_columns(self, file_path, encoding, separator):
        """检测哪些列应该作为字符串读取（避免长数字被转换为科学计数法）"""
        try:
            # 直接读取文件的前几行文本，避免pandas自动转换
            with open(file_path, 'r', encoding=encoding) as f:
                lines = []
                for i, line in enumerate(f):
                    lines.append(line.strip())
                    if i >= 20:  # 读取更多行以提高检测准确性
                        break
            
            if len(lines) < 2:
                return {}
            
            # 解析标题行
            headers = lines[0].split(separator)
            string_columns = {}
            
            # 检查每一列的数据
            for col_idx, col_name in enumerate(headers):
                col_name = col_name.strip().strip('"')  # 去除可能的引号
                
                # 检查这一列的数据值
                should_be_string = False
                sample_values = []
                
                for line in lines[1:]:  # 跳过标题行
                    values = line.split(separator)
                    if col_idx < len(values):
                        value = values[col_idx].strip().strip('"')  # 去除引号和空格
                        
                        if value:  # 非空值
                            sample_values.append(value)
                            
                            # 检查是否为长数字（超过15位的数字，会导致精度丢失）
                            if value.isdigit() and len(value) > 15:
                                should_be_string = True
                                self.log_merge(f"检测到超长数字列: {col_name} (示例值: {value}, 长度: {len(value)})")
                                break
                            # 检查是否为可能丢失精度的长数字（10-15位）
                            elif value.isdigit() and len(value) >= 10:
                                # 对于10-15位的数字，也建议作为字符串处理以避免潜在的精度问题
                                should_be_string = True
                                self.log_merge(f"检测到长数字列: {col_name} (示例值: {value}, 长度: {len(value)})")
                                break
                            # 检查是否包含前导零的数字
                            elif value.startswith('0') and value.isdigit() and len(value) > 1:
                                should_be_string = True
                                self.log_merge(f"检测到前导零列: {col_name} (示例值: {value})")
                                break

                
                if should_be_string:
                    string_columns[col_name] = str
            
            return string_columns
        except Exception as e:
            self.log_merge(f"列类型检测失败: {str(e)}")
            return {}

    def detect_excel_string_columns(self, df_sample):
        """检测Excel文件中哪些列应该作为字符串读取"""
        string_columns = {}
        
        for col_name in df_sample.columns:
            col_data = df_sample[col_name].dropna()
            
            for value in col_data:
                # 转换为字符串进行检查
                str_value = str(value)
                
                # 检查是否为长数字
                if str_value.replace('.', '').replace('e', '').replace('E', '').replace('+', '').replace('-', '').isdigit():
                    # 检查科学计数法
                    if 'e' in str_value.lower() or 'E' in str_value:
                        string_columns[col_name] = str
                        self.log_merge(f"Excel中检测到科学计数法列: {col_name} (示例值: {str_value})")
                        break
                    # 检查长数字
                    elif str_value.isdigit() and len(str_value) >= 10:
                        string_columns[col_name] = str
                        self.log_merge(f"Excel中检测到长数字列: {col_name} (示例值: {str_value})")
                        break
                
                # 检查常见的ID字段名称
                if col_name.upper() in ['IMEI', 'ICCID', 'IMSI', 'ID', 'SERIAL', 'BARCODE', 'CODE']:
                    if str_value.isdigit() and len(str_value) >= 8:
                        string_columns[col_name] = str
                        self.log_merge(f"Excel中检测到ID类型列: {col_name} (示例值: {str_value})")
                        break
        
        return string_columns

    def detect_file_format(self, file_path):
        """检测文件的实际格式"""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(512)  # 读取文件头
                
            # 检查Excel文件的魔术字节
            if header.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1'):  # OLE2格式 (.xls)
                return 'excel_xls'
            elif header.startswith(b'PK\x03\x04'):  # ZIP格式 (.xlsx)
                return 'excel_xlsx'
            else:
                # 尝试检测是否为文本文件（CSV等）
                try:
                    # 尝试用不同编码解码前几行
                    encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'latin1']
                    for encoding in encodings_to_try:
                        try:
                            text = header.decode(encoding)
                            # 检查是否包含常见的CSV分隔符
                            if any(sep in text for sep in [',', '\t', ';', '|']):
                                return 'text_csv'
                            # 检查是否为纯文本
                            if all(ord(c) < 128 or c.isprintable() for c in text if c != '\x00'):
                                return 'text_plain'
                        except UnicodeDecodeError:
                            continue
                    return 'unknown'
                except:
                    return 'unknown'
        except Exception as e:
            self.log_merge(f"文件格式检测失败 {file_path}: {str(e)}")
            return 'unknown' 
   
    # 合并功能（整合hb ec.py的高级功能）
    def start_merge_thread(self):
        if self.merging:
            return

        input_dir = self.merge_input_path.get()
        output_file = self.merge_output_path.get()

        if not input_dir or not output_file:
            messagebox.showerror("错误", "请选择输入文件夹和输出文件路径")
            return

        if not os.path.isdir(input_dir):
            messagebox.showerror("错误", "输入文件夹不存在或无效")
            return

        # 在后台线程中执行合并操作
        self.merging = True
        self.merge_btn.config(state=tk.DISABLED)
        self.log_merge("开始合并文件...")
        self.merge_log_text.config(state=tk.NORMAL)
        self.merge_log_text.delete(1.0, tk.END)
        self.merge_log_text.config(state=tk.DISABLED)
        self.merge_progress["value"] = 0

        threading.Thread(target=self.merge_files, args=(input_dir, output_file), daemon=True).start()

    def merge_files(self, input_dir, output_file):
        try:
            # 获取所有CSV和Excel文件
            all_files = []
            for ext in ["*.csv", "*.xlsx", "*.xls"]:
                all_files.extend([os.path.join(input_dir, f) for f in os.listdir(input_dir)
                                  if f.lower().endswith(tuple(ext.split('.')[-1].split('|')))
                                  and not os.path.basename(f).startswith('~')])  # 忽略临时文件

            if not all_files:
                self.log_merge("错误: 未找到任何CSV或Excel文件")
                return

            total_files = len(all_files)
            self.log_merge(f"找到 {total_files} 个文件进行合并")

            # 读取并合并文件
            dfs = []
            success_count = 0
            fail_count = 0

            for i, file_path in enumerate(all_files):
                filename = os.path.basename(file_path)
                try:
                    # 智能检测文件格式
                    actual_format = self.detect_file_format(file_path)
                    self.log_merge(f"检测到文件格式: {filename} -> {actual_format}")
                    
                    if file_path.lower().endswith(('.csv')) or actual_format in ['text_csv', 'text_plain']:
                        # 处理CSV或文本文件
                        if self.merge_encoding.get() == "自动检测":
                            encoding = self.detect_file_encoding(file_path)
                        else:
                            encoding = self.merge_encoding.get()

                        # 尝试不同的分隔符
                        separators = [',', '\t', ';', '|']
                        df = None
                        used_sep = ','
                        
                        for sep in separators:
                            try:
                                df_test = pd.read_csv(file_path, encoding=encoding, sep=sep, nrows=5)
                                if len(df_test.columns) > 1:  # 如果能正确分割出多列
                                                # 直接将所有列读取为字符串，避免长数字处理问题
                                    df = pd.read_csv(file_path, encoding=encoding, sep=sep, dtype=str)
                                    used_sep = sep
                                    break
                            except:
                                continue
                        
                        if df is None:
                            # 如果所有分隔符都失败，尝试自动检测
                            try:
                                df = pd.read_csv(file_path, encoding=encoding, dtype=str)
                            except:
                                df = pd.read_csv(file_path, encoding=encoding)
                            used_sep = '自动'

                        self.log_merge(f"已加载: {filename} (编码: {encoding}, 分隔符: {repr(used_sep)}, 所有列作为字符串)")
                        
                    elif actual_format == 'excel_xls':
                        # 真正的.xls文件
                        # 直接将所有列读取为字符串，避免长数字处理问题
                        df = pd.read_excel(file_path, engine='xlrd', dtype=str)
                        self.log_merge(f"已加载: {filename} (Excel .xls格式)")
                        
                    elif actual_format == 'excel_xlsx':
                        # 真正的.xlsx文件
                        # 直接将所有列读取为字符串，避免长数字处理问题
                        df = pd.read_excel(file_path, engine='openpyxl', dtype=str)
                        self.log_merge(f"已加载: {filename} (Excel .xlsx格式)")
                        
                    else:
                        # 对于扩展名为.xls/.xlsx但实际不是Excel格式的文件，尝试作为文本处理
                        if file_path.lower().endswith(('.xls', '.xlsx')):
                            self.log_merge(f"警告: {filename} 扩展名为Excel但实际不是Excel格式，尝试作为文本文件处理")
                            
                            if self.merge_encoding.get() == "自动检测":
                                encoding = self.detect_file_encoding(file_path)
                            else:
                                encoding = self.merge_encoding.get()
                            
                            # 尝试不同的分隔符
                            separators = [',', '\t', ';', '|']
                            df = None
                            used_sep = ','
                            
                            for sep in separators:
                                try:
                                    df_test = pd.read_csv(file_path, encoding=encoding, sep=sep, nrows=5)
                                    if len(df_test.columns) > 1:
                                        # 直接将所有列读取为字符串，避免长数字处理问题
                                        df = pd.read_csv(file_path, encoding=encoding, sep=sep, dtype=str)
                                        used_sep = sep
                                        break
                                except:
                                    continue
                            
                            if df is None:
                                try:
                                    df = pd.read_csv(file_path, encoding=encoding, dtype=str)
                                except:
                                    df = pd.read_csv(file_path, encoding=encoding)
                                used_sep = '自动'
                                
                            self.log_merge(f"已加载: {filename} (作为文本文件, 编码: {encoding}, 分隔符: {repr(used_sep)}, 所有列作为字符串)")
                        else:
                            raise Exception(f"不支持的文件格式: {actual_format}")

                    # 添加来源文件列
                    df['来源文件'] = filename

                    dfs.append(df)
                    success_count += 1
                except Exception as e:
                    self.log_merge(f"错误加载 {filename}: {str(e)}")
                    fail_count += 1

                # 更新进度条
                self.merge_progress["value"] = (i + 1) / total_files * 100
                self.root.update_idletasks()

            if not dfs:
                self.log_merge("错误: 没有成功加载任何文件")
                return

            combined_df = pd.concat(dfs, ignore_index=True)

            # 根据输出文件扩展名决定保存格式
            if output_file.lower().endswith('.csv'):
                combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                self.log_merge("保存为CSV格式")
            elif output_file.lower().endswith(('.xlsx', '.xls')):
                combined_df.to_excel(output_file, index=False)
                self.log_merge("保存为Excel格式")
            else:
                # 默认保存为CSV
                combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                self.log_merge("未识别扩展名，默认保存为CSV格式")

            self.log_merge(f"合并完成! 已保存到: {output_file}")
            self.log_merge(f"总行数: {len(combined_df)}")
            self.log_merge(f"成功合并文件: {success_count}个, 失败: {fail_count}个")

            messagebox.showinfo("成功",
                                f"文件合并完成!\n\n"
                                f"合并文件数: {success_count}\n"
                                f"失败文件数: {fail_count}\n"
                                f"总行数: {len(combined_df)}\n"
                                f"保存到: {output_file}")

        except Exception as e:
            self.log_merge(f"合并过程中出错: {str(e)}")
            messagebox.showerror("错误", f"合并失败: {str(e)}")
        finally:
            self.merging = False
            self.merge_btn.config(state=tk.NORMAL)
            self.merge_progress["value"] = 100  
  
    # 分解功能
    def start_splitting(self):
        if not self.split_input_file.get():
            messagebox.showerror("错误", "请选择输入文件")
            return
            
        if not self.split_output_dir.get():
            messagebox.showerror("错误", "请选择输出目录")
            return
            
        thread = threading.Thread(target=self.split_files)
        thread.daemon = True
        thread.start()
        
    def split_files(self):
        try:
            filename = self.split_input_file.get()
            output_dir = self.split_output_dir.get()
            
            self.split_status_label.config(text="正在读取文件...")
            self.split_progress["value"] = 0
            self.log_split("开始分解文件...")
            
            file_extension = os.path.splitext(filename)[1].lower()
            
            # 根据文件类型读取，所有列都作为字符串处理，避免科学计数法问题
            if file_extension == '.csv':
                encoding = self.detect_file_encoding(filename)
                
                # 尝试不同的分隔符，所有列都作为字符串读取
                separators = [',', '\t', ';', '|']
                df = None
                used_sep = ','
                
                for sep in separators:
                    try:
                        df = pd.read_csv(filename, encoding=encoding, sep=sep, dtype=str)
                        used_sep = sep
                        break
                    except:
                        continue
                
                if df is None:
                    # 如果所有分隔符都失败，尝试自动检测分隔符
                    try:
                        df = pd.read_csv(filename, encoding=encoding, dtype=str)
                    except:
                        df = pd.read_csv(filename, encoding=encoding)
                    used_sep = '自动'
                    
                self.log_split(f"已加载文件: {os.path.basename(filename)} (编码: {encoding}, 分隔符: {repr(used_sep)}, 所有列作为字符串)")
                
            elif file_extension == '.xls':
                # 真正的.xls文件，所有列都作为字符串读取
                df = pd.read_excel(filename, engine='xlrd', dtype=str)
                self.log_split(f"已加载文件: {os.path.basename(filename)} (Excel .xls格式, 所有列作为字符串)")
                
            elif file_extension == '.xlsx':
                # 真正的.xlsx文件，所有列都作为字符串读取
                df = pd.read_excel(filename, engine='openpyxl', dtype=str)
                self.log_split(f"已加载文件: {os.path.basename(filename)} (Excel .xlsx格式, 所有列作为字符串)")
                
            else:
                messagebox.showerror("错误", "不支持的文件格式")
                return
                
            total_rows = len(df)
            if total_rows == 0:
                messagebox.showwarning("警告", "文件为空")
                return
                
            # 确定条码列
            barcode_col = self.split_barcode_column.get() if self.split_barcode_column.get() else df.columns[0]
            if barcode_col not in df.columns:
                messagebox.showerror("错误", f"列 '{barcode_col}' 不存在")
                return
                
            # 创建输出目录
            os.makedirs(output_dir, exist_ok=True)
            
            # 按条码列分组并保存
            processed = 0
            groups = df.groupby(barcode_col)
            total_groups = len(groups)
            
            for barcode, group in groups:
                barcode = str(barcode).strip()
                if not barcode:
                    barcode = f"row_{processed + 1}"
                    
                # 清理文件名
                barcode = re.sub(r'[\\/:*?"<>|]', '_', barcode)
                barcode = barcode[:50]
                
                output_filename = f"{barcode}.xlsx"
                output_path = os.path.join(output_dir, output_filename)
                
                try:
                    group.to_excel(output_path, index=False)
                    self.log_split(f"已创建: {output_filename}")
                except Exception as e:
                    self.log_split(f"创建文件失败 {output_filename}: {str(e)}")
                    
                processed += 1
                progress = (processed / total_groups) * 100
                self.split_progress["value"] = progress
                self.split_status_label.config(text=f"处理中... {processed}/{total_groups}")
                
            self.log_split(f"分解完成! 共创建 {total_groups} 个文件")
            self.split_status_label.config(text="完成")
            
        except Exception as e:
            self.log_split(f"分解过程中出现错误: {str(e)}")
            self.split_status_label.config(text="错误")
    
    # 上传监控相关方法
    def process_upload_queue(self):
        """处理上传队列消息"""
        try:
            while True:
                msg = self.upload_ui_queue.get_nowait()
                if msg[0] == 'status':
                    self.upload_status_label.config(text=msg[1])
                elif msg[0] == 'log':
                    self.upload_log_listbox.insert(tk.END, msg[1])
                    self.upload_log_listbox.yview(tk.END)
                elif msg[0] == 'progress':
                    self.upload_progress_bar['value'] = msg[1]
                elif msg[0] == 'progress_max':
                    self.upload_progress_bar['maximum'] = msg[1]
                elif msg[0] == 'show_error':
                    messagebox.showerror(msg[1], msg[2])
                elif msg[0] == 'monitoring_stopped':
                    self.stop_upload_monitoring(from_thread=True)
        except queue.Empty:
            pass
        self.root.after(100, self.process_upload_queue)

    def load_upload_history(self):
        """加载上传历史"""
        self.uploaded_files_set.clear()
        if os.path.exists(self.upload_history_file_path):
            try:
                with open(self.upload_history_file_path, 'r', encoding='utf-8') as f:
                    self.uploaded_files_set = set(line.strip() for line in f if line.strip())
                self.upload_ui_queue.put(('log', f"成功加载 {len(self.uploaded_files_set)} 条上传历史。"))
            except Exception as e:
                self.upload_ui_queue.put(('log', f"错误: 无法加载历史文件: {e}"))
        else:
            self.upload_ui_queue.put(('log', "未找到历史文件，将创建新的记录。"))

    def select_upload_folder(self):
        """选择监控文件夹"""
        if self.is_upload_monitoring:
            messagebox.showwarning("正在监控", "请先停止当前监控任务。")
            return
        folder = filedialog.askdirectory(title="选择要监控的文件夹")
        if folder:
            self.upload_monitoring_folder = folder
            self.upload_folder_label.config(text=f"监控目标: {self.upload_monitoring_folder}")
            self.upload_history_file_path = os.path.join(self.upload_monitoring_folder, ".upload_history.log")
            self.upload_log_listbox.delete(0, tk.END)
            self.load_upload_history()
            self.upload_status_label.config(text="文件夹已选择。点击'开始监控'以上传。")

    def select_upload_destination(self):
        """选择上传目标文件夹"""
        if self.is_upload_monitoring:
            messagebox.showwarning("正在监控", "请先停止当前监控任务。")
            return
        folder = filedialog.askdirectory(title="选择上传目标文件夹")
        if folder:
            self.upload_destination_path = folder
            self.upload_dest_label.config(text=f"上传至: {self.upload_destination_path}")
            self.upload_status_label.config(text="上传位置已选择。")

    def start_upload_monitoring(self):
        """开始上传监控"""
        if not self.upload_monitoring_folder:
            messagebox.showwarning("未选择文件夹", "请先选择一个要监控的文件夹。")
            return
        if not self.upload_destination_path:
            messagebox.showwarning("未选择上传位置", "请先选择上传目标文件夹。")
            return
        if not os.path.isdir(self.upload_destination_path):
            messagebox.showerror("错误", f"目标路径不存在或不是一个文件夹:\n{self.upload_destination_path}")
            return

        self.is_upload_monitoring = True
        self.select_source_button.config(state=tk.DISABLED)
        self.select_dest_button.config(state=tk.DISABLED)
        self.upload_start_button.config(state=tk.DISABLED)
        self.upload_stop_button.config(state=tk.NORMAL)
        self.upload_status_label.config(text=f"正在准备监控: {self.upload_monitoring_folder}")

        self.upload_monitoring_thread = threading.Thread(target=self.monitor_upload_folder)
        self.upload_monitoring_thread.daemon = True
        self.upload_monitoring_thread.start()

    def stop_upload_monitoring(self, from_thread=False):
        """停止上传监控"""
        if self.is_upload_monitoring:
            self.is_upload_monitoring = False
            if not from_thread:
                self.upload_ui_queue.put(('status', "正在停止监控..."))

            self.select_source_button.config(state=tk.NORMAL)
            self.select_dest_button.config(state=tk.NORMAL)
            self.upload_start_button.config(state=tk.NORMAL)
            self.upload_stop_button.config(state=tk.DISABLED)
            self.upload_status_label.config(text="监控已停止。")
            self.upload_folder_label.config(text=f"已停止监控: {self.upload_monitoring_folder}")
            self.upload_progress_bar['value'] = 0

    def monitor_upload_folder(self):
        """监控文件夹并上传文件"""
        self.upload_ui_queue.put(('log', "开始初次扫描..."))
        try:
            history_filename = os.path.basename(self.upload_history_file_path)
            all_current_files = {
                f for f in os.listdir(self.upload_monitoring_folder)
                if os.path.isfile(os.path.join(self.upload_monitoring_folder, f)) and f != history_filename
            }
            files_to_upload_initially = list(all_current_files - self.uploaded_files_set)

            if files_to_upload_initially:
                self.upload_ui_queue.put(('log', f"发现 {len(files_to_upload_initially)} 个未上传的文件。"))
            else:
                self.upload_ui_queue.put(('log', "文件已是最新，无需初次上传。"))

        except Exception as e:
            self.upload_ui_queue.put(('show_error', "错误", f"无法读取监控文件夹: {e}"))
            self.upload_ui_queue.put(('monitoring_stopped', None))
            return

        self.upload_ui_queue.put(('progress', 0))
        self.upload_ui_queue.put(('progress_max', len(files_to_upload_initially)))

        for i, filename in enumerate(files_to_upload_initially):
            if not self.is_upload_monitoring:
                self.upload_ui_queue.put(('log', "监控在初次扫描期间停止。"))
                return
            self._upload_single_file(filename)
            self.upload_ui_queue.put(('progress', i + 1))

        self.upload_ui_queue.put(('log', "初次扫描完成。开始实时监控..."))
        self.upload_ui_queue.put(('status', "实时监控中..."))
        self.upload_ui_queue.put(('progress', 0))

        while self.is_upload_monitoring:
            try:
                history_filename = os.path.basename(self.upload_history_file_path)
                current_files = set(
                    f for f in os.listdir(self.upload_monitoring_folder)
                    if os.path.isfile(os.path.join(self.upload_monitoring_folder, f)) and f != history_filename
                )
                new_files = current_files - self.uploaded_files_set

                if new_files:
                    for filename in new_files:
                        if not self.is_upload_monitoring:
                            break
                        self._upload_single_file(filename, is_new=True)
                    self.upload_ui_queue.put(('status', "实时监控中..."))
                time.sleep(5)
            except Exception as e:
                self.upload_ui_queue.put(('log', f"监控循环中发生错误: {e}"))
                time.sleep(10)
        self.upload_ui_queue.put(('log', "监控线程已结束。"))

    def _upload_single_file(self, filename, is_new=False):
        """上传单个文件"""
        status_prefix = "发现新文件: " if is_new else "上传初始文件: "
        log_prefix = "新文件已上传: " if is_new else "已上传: "

        self.upload_ui_queue.put(('status', f"{status_prefix}{filename}"))
        file_path = os.path.join(self.upload_monitoring_folder, filename)
        dest_file = os.path.join(self.upload_destination_path, filename)
        try:
            shutil.copy2(file_path, dest_file)
            self.uploaded_files_set.add(filename)
            
            # 写入历史文件
            try:
                with open(self.upload_history_file_path, 'a', encoding='utf-8') as f:
                    f.write(filename + '\n')
            except Exception as e:
                self.upload_ui_queue.put(('log', f"警告: 无法写入历史记录: {e}"))

            self.upload_ui_queue.put(('log', f"{log_prefix}{filename}"))
        except Exception as e:
            self.upload_ui_queue.put(('log', f"错误: 上传 {filename} 失败: {e}"))


if __name__ == "__main__":
    root = tk.Tk()
    app = DataProcessorApp(root)
    root.mainloop()