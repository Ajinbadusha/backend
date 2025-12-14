"""
AI Enrichment Pipeline
- Vision: Image captions + attribute extraction
- Text: Normalization + attribute extraction
"""

import openai
import json
import base64
import requests
from typing import Dict, List
import os

openai.api_key = os.getenv('OPENAI_API_KEY')


class AIEnrichment:
    """Enrich products with AI-generated descriptions and attributes"""
    
    VISION_PROMPT = """
    Analyze this ecommerce product image and provide:
    1. A detailed caption describing what you see (1-2 sentences)
    2. Extract structured attributes in JSON:
    {
        "category": "clothing/electronics/furniture/etc",
        "colors": ["color1", "color2"],
        "material": "cotton/leather/metal/etc",
        "pattern": "solid/striped/floral/etc",
        "condition": "new/used/refurbished",
        "style": "casual/formal/sporty/etc",
        "occasion": "casual/office/party/gym/etc"
    }
    
    Return JSON with "caption" and "attributes" keys.
    """
    
    async def enrich_product(self, product: Dict) -> Dict:
        """Main enrichment function"""
        enrichment = {
            'visual_summary': '',
            'attributes': {
                'category': 'unknown',
                'colors': [],
                'material': 'unknown',
                'pattern': 'unknown',
            },
            'per_image': {},
            'enriched_text': ''
        }
        
        # Process each image
        image_data = []
        for i, img_url in enumerate(product.get('images', [])[:3]):
            try:
                img_result = await self._process_image(img_url)
                image_data.append(img_result)
                enrichment['per_image'][i] = img_result
            except Exception as e:
                print(f"Error processing image {img_url}: {e}")
        
        # Process text
        text_result = await self._process_text(product.get('description', ''))
        enrichment['attributes'].update(text_result)
        
        # Create visual summary
        if image_data:
            captions = [d.get('caption', '') for d in image_data]
            enrichment['visual_summary'] = ' | '.join(captions)
        
        # Create unified search document
        enrichment['enriched_text'] = f"""
        Title: {product.get('title', '')}
        Description: {product.get('description', '')}
        Visual Summary: {enrichment['visual_summary']}
        Attributes: {json.dumps(enrichment['attributes'])}
        """.strip()
        
        return enrichment
    
    async def _process_image(self, image_url: str) -> Dict:
        """SOW 2.5.A - Vision enrichment"""
        try:
            # Call OpenAI GPT-4 Vision
            response = openai.chat.completions.create(
                model="gpt-4-vision-preview",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": self.VISION_PROMPT},
                            {"type": "image_url", "image_url": {"url": image_url}}
                        ]
                    }
                ],
                max_tokens=500
            )
            
            result_text = response.choices[0].message.content
            
            # Parse JSON from response
            json_match = result_text.find('{')
            if json_match >= 0:
                json_str = result_text[json_match:]
                json_end = json_str.rfind('}') + 1
                attributes = json.loads(json_str[:json_end])
                caption = result_text[:json_match].strip()
            else:
                caption = result_text
                attributes = {}
            
            return {
                'image_url': image_url,
                'caption': caption,
                'attributes': attributes
            }
        
        except Exception as e:
            print(f"OpenAI Vision error: {e}")
            return {
                'image_url': image_url,
                'caption': 'Image processed',
                'attributes': {}
            }
    
    async def _process_text(self, description: str) -> Dict:
        """SOW 2.5.B - Text enrichment"""
        if not description:
            return {}
        
        try:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "user",
                        "content": f"""
                        Extract structured attributes from this product description:
                        
                        {description}
                        
                        Return JSON with fields: category, materials, colors, size_range, care_instructions
                        Keep unknown fields as null.
                        """
                    }
                ],
                max_tokens=300
            )
            
            result_text = response.choices[0].message.content
            json_start = result_text.find('{')
            if json_start >= 0:
                attributes = json.loads(result_text[json_start:])
                return attributes
        except:
            pass
        
        return {}

