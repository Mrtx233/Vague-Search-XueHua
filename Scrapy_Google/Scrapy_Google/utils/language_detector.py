import os
import logging
from typing import Tuple, List

try:
    import fasttext
except ImportError:
    fasttext = None

logger = logging.getLogger(__name__)


class LanguageDetector:
    LANG_NAME_ZH = {
        "zh": "中文",
        "en": "英语",
        "es": "西班牙语",
        "fr": "法语",
        "de": "德语",
        "pt": "葡萄牙语",
        "it": "意大利语",
        "ru": "俄语",
        "ja": "日语",
        "ko": "韩语",
        "ar": "阿拉伯语",
        "hi": "印地语",
        "tr": "土耳其语",
        "nl": "荷兰语",
        "sv": "瑞典语",
        "no": "挪威语",
        "da": "丹麦语",
        "fi": "芬兰语",
        "pl": "波兰语",
        "cs": "捷克语",
        "el": "希腊语",
        "he": "希伯来语",
        "th": "泰语",
        "vi": "越南语",
        "id": "印尼语",
        "ms": "马来语",
        "uk": "乌克兰语",
        "ro": "罗马尼亚语",
        "hu": "匈牙利语",
        "bg": "保加利亚语",
        "sr": "塞尔维亚语",
        "hr": "克罗地亚语",
        "sk": "斯洛伐克语",
        "sl": "斯洛文尼亚语",
        "et": "爱沙尼亚语",
        "lv": "拉脱维亚语",
        "lt": "立陶宛语",
        "fa": "波斯语",
        "ur": "乌尔都语",
        "bn": "孟加拉语",
        "ta": "泰米尔语",
        "te": "泰卢固语",
        "mr": "马拉地语",
        "gu": "古吉拉特语",
        "pa": "旁遮普语",
        "sw": "斯瓦希里语",
    }

    def __init__(self, model_path: str, confidence_threshold: float = 0.8):
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.model = None
        self._load_model()

    def _load_model(self) -> None:
        if fasttext is None:
            logger.error("fasttext not installed; language detection disabled")
            return
        if not os.path.exists(self.model_path):
            logger.error(f"FastText model not found: {self.model_path}")
            logger.error("Download from https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin")
            return
        try:
            fasttext.FastText.eprint = lambda x: None
            self.model = fasttext.load_model(self.model_path)
            logger.info(f"FastText model loaded: {self.model_path}")
        except Exception as e:
            logger.error(f"FastText load failed: {str(e)}")
            self.model = None

    def is_model_loaded(self) -> bool:
        return self.model is not None

    @staticmethod
    def _normalize_code(code: str) -> str:
        if not code:
            return "unknown"
        c = code.strip().lower()
        if c.startswith("__label__"):
            c = c.replace("__label__", "")
        return c or "unknown"

    def lang_code_to_zh_name(self, code: str) -> str:
        c = self._normalize_code(code)
        if c == "unknown":
            return "未知"
        return self.LANG_NAME_ZH.get(c, f"未知({c})")

    def detect_language(self, text: str) -> Tuple[str, float]:
        if not self.model:
            logger.warning("FastText model is not loaded")
            return 'unknown', 0.0
        processed_text = text.strip().replace("\n", " ").replace("\r", "")
        if not processed_text:
            return 'unknown', 0.0
        try:
            labels, probs = self.model.predict(processed_text, k=1)
            lang_code = self._normalize_code(labels[0])
            confidence = round(float(probs[0]), 4)
            return lang_code, confidence
        except Exception as e:
            logger.error(f"Language detection failed: {str(e)[:50]}")
            return 'unknown', 0.0

    def detect_with_threshold(self, text: str) -> str:
        lang_code, confidence = self.detect_language(text)
        if confidence >= self.confidence_threshold:
            return lang_code
        return 'unknown'

    def detect_with_threshold_zh(self, text: str) -> str:
        code = self.detect_with_threshold(text)
        return self.lang_code_to_zh_name(code)

    def detect_language_zh(self, text: str) -> Tuple[str, float]:
        code, conf = self.detect_language(text)
        return self.lang_code_to_zh_name(code), conf

    def batch_detect(self, texts: List[str]) -> List[Tuple[str, float]]:
        if not self.model:
            return [('unknown', 0.0)] * len(texts)
        return [self.detect_language(text) for text in texts]

    def get_supported_languages(self) -> List[str]:
        if not self.model:
            return []
        try:
            labels = self.model.get_labels()
            languages = [self._normalize_code(label) for label in labels]
            return sorted(set(languages))
        except Exception as e:
            logger.error(f"Failed to read supported languages: {str(e)}")
            return []

    def reload_model(self) -> bool:
        self._load_model()
        return self.is_model_loaded()

    def set_confidence_threshold(self, threshold: float) -> None:
        if 0.0 <= threshold <= 1.0:
            self.confidence_threshold = threshold
        else:
            logger.warning(f"Invalid confidence threshold: {threshold}")

    def get_model_info(self) -> dict:
        if not self.model:
            return {"loaded": False, "path": self.model_path}
        try:
            return {
                "loaded": True,
                "path": self.model_path,
                "confidence_threshold": self.confidence_threshold,
                "supported_languages_count": len(self.get_supported_languages())
            }
        except Exception:
            return {"loaded": True, "path": self.model_path, "error": "failed to read model info"}
