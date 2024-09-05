import os
import logging
import PyPDF2
import io
import argparse

logger = logging.getLogger(__name__)

from magic_pdf.libs.version import __version__
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.rw import AbsReaderWriter
from magic_pdf.pdf_parse_by_ocr import parse_pdf_by_ocr
from magic_pdf.pdf_parse_by_txt import parse_pdf_by_txt
from magic_pdf.rw.AbsReaderWriter import AbsReaderWriter 
from magic_pdf.pipe.AbsPipe import AbsPipe

PARSE_TYPE_TXT = "txt"
PARSE_TYPE_OCR = "ocr"

def parse_txt_pdf(pdf_bytes: bytes, pdf_models: list, imageWriter: AbsReaderWriter, is_debug=False, start_page=0, *args,
                  **kwargs):
    """
    解析文本类pdf
    """
    pdf_info_dict = parse_pdf_by_txt(
        pdf_bytes,
        pdf_models,
        imageWriter,
        start_page_id=start_page,
        debug_mode=is_debug,
    )

    pdf_info_dict["_parse_type"] = PARSE_TYPE_TXT

    pdf_info_dict["_version_name"] = __version__

    return pdf_info_dict


def parse_ocr_pdf(pdf_bytes: bytes, pdf_models: list, imageWriter: AbsReaderWriter, is_debug=False, start_page=0, *args,
                  **kwargs):
    """
    解析ocr类pdf
    """
    pdf_info_dict = parse_pdf_by_ocr(
        pdf_bytes,
        pdf_models,
        imageWriter,
        start_page_id=start_page,
        debug_mode=is_debug,
    )

    pdf_info_dict["_parse_type"] = PARSE_TYPE_OCR

    pdf_info_dict["_version_name"] = __version__

    return pdf_info_dict


def parse_union_pdf(pdf_bytes: bytes, pdf_models: list, imageWriter: AbsReaderWriter, is_debug=False, start_page=0,
                    input_model_is_empty: bool = False,
                    *args, **kwargs):
    """
    ocr和文本混合的pdf，全部解析出来
    """

    def parse_pdf(method):
        try:
            return method(
                pdf_bytes,
                pdf_models,
                imageWriter,
                start_page_id=start_page,
                debug_mode=is_debug,
            )
        except Exception as e:
            logger.exception(e)
            return None

    pdf_info_dict = parse_pdf(parse_pdf_by_txt)
    if pdf_info_dict is None or pdf_info_dict.get("_need_drop", False):
        logger.warning(f"parse_pdf_by_txt drop or error, switch to parse_pdf_by_ocr")
        if input_model_is_empty:
            pdf_models = doc_analyze(pdf_bytes, ocr=True)
        pdf_info_dict = parse_pdf(parse_pdf_by_ocr)
        if pdf_info_dict is None:
            raise Exception("Both parse_pdf_by_txt and parse_pdf_by_ocr failed.")
        else:
            pdf_info_dict["_parse_type"] = PARSE_TYPE_OCR
    else:
        pdf_info_dict["_parse_type"] = PARSE_TYPE_TXT

    pdf_info_dict["_version_name"] = __version__

    return pdf_info_dict


def process_pdf_directory(directory_path, imageWriter, start_page=0, is_debug=False):
    pdf_files = [f for f in os.listdir(directory_path) if f.endswith('.pdf')]
    total_files = len(pdf_files)
    
    for index, pdf_file in enumerate(pdf_files):
        pdf_path = os.path.join(directory_path, pdf_file)
        with open(pdf_path, 'rb') as f:
            pdf_bytes = f.read()
        # 判断PDF文件类型
        pdf_type = determine_pdf_type(pdf_path)
        logger.info(f"PDF类型: {pdf_type}")
        
        pdf_models = doc_analyze(pdf_bytes=pdf_bytes)
        if pdf_type == "normal":
            pdf_info_dict = parse_txt_pdf(pdf_bytes, pdf_models, imageWriter, start_page, is_debug)
        elif pdf_type == "scanned":
            pdf_info_dict = parse_ocr_pdf(pdf_bytes, pdf_models, imageWriter, start_page, is_debug)
        elif pdf_type == "mix_pdf":
            pdf_info_dict = parse_union_pdf(pdf_bytes, pdf_models, imageWriter, start_page, is_debug)
        else:
            print(f"unknown PDF type: {pdf_type}")
            continue
        # 显示进度
        logger.info(f"files Processing {index + 1}/{total_files}: {pdf_file}")
        
        # 处理后的pdf_info_dict可以在这里进一步使用或保存
def determine_pdf_type(pdf_path):
    pdf_type = "mix_pdf"  # 默认为混合类型
    text_pages = 0
    scanned_pages = 0

    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            total_pages = len(pdf_reader.pages)

            for page in pdf_reader.pages:
                text = page.extract_text().strip()
                if text:
                    text_pages += 1
                else:
                    scanned_pages += 1

            if text_pages == total_pages:
                pdf_type = "normal"
            elif scanned_pages == total_pages:
                pdf_type = "scanned"
            # 否则保持为混合类型

    except Exception as e:
        print(f"处理PDF时发生错误：{str(e)}")
        pdf_type = "unknown"

    return pdf_type

class AbsPipe(ABC):
    """
    txt和ocr处理的抽象类
    """
    PIP_OCR = "ocr"
    PIP_TXT = "txt"

    def __init__(self, pdf_bytes: bytes, model_list: list, image_writer: AbsReaderWriter, is_debug: bool = False):
        self.pdf_bytes = pdf_bytes
        self.model_list = model_list
        self.image_writer = image_writer
        self.pdf_mid_data = None  # 未压缩
        self.is_debug = is_debug
if __name__ =="__main__":
    pdf_path = "E:\大语言模型理论资料"
    # read from command line use argprase 
    #parser = argparse.ArgumentParser()
    # parser.add_argument("--pdf_path", type=str, help="PDF文件路径")
    # args = parser.parse_args()
    # pdf_path = args.pdf_path
    
    process_pdf_directory(pdf_path, imageWriter, start_page=0, is_debug=False)