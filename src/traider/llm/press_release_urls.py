from dspy.signatures import Signature, InputField, OutputField
from dspy import Predict, LM, configure
import os
from dotenv import load_dotenv

load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
lm = LM(model="gpt-4.1-mini", api_key=openai_api_key)
configure(lm=lm)



class PressReleaseUrl(Signature):
    """From the list below, select the root URL for the company's Press Releases with a priority for earnings report (financial reporting).
    These are for tracking stock-relevant press release news, specifically earnings reports.
    """

    input_urls: list[str] = InputField(desc="The list of URLs to select from")
    output_url: str = OutputField(desc="The selected URL")

select_press_release_url = Predict(PressReleaseUrl)
