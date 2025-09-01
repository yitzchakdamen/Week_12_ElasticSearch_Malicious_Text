from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging

logger = logging.getLogger(__name__)

class Analysis:

    @staticmethod
    def analyze_sentiment(text: str) -> float:
        """Analyze the sentiment of a tweet."""
        logger.debug(f"Analyzing sentiment for tweet: {text[:50]}....")
        return SentimentIntensityAnalyzer().polarity_scores(text)["compound"]
    
    @staticmethod
    def sentiment_category(num:float) -> str:
        """Categorize the sentiment score."""
        if num >= 0.5: return "Positive"
        elif num <= -0.5: return "Negative"
        else: return "Neutral"
        
    @staticmethod
    def weapons_detected(text: str, weapons:list) -> list[str]:
        """Detect weapons mentioned in the text."""
        return [weapon for weapon in weapons if weapon in text.split()]