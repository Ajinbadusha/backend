"""
AI Enrichment Pipeline
- Vision: Image captions + attribute extraction (Note: Groq doesn't support vision, using text-only)
- Text: Normalization + attribute extraction
"""

import json
from typing import Dict, List
import os
from dotenv import load_dotenv

from groq import Groq

# Load environment variables from .env file
load_dotenv()


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
                
                # FIX 1: Merge attributes from vision model into main attributes
                vision_attrs = img_result.get('attributes', {})
                for key, value in vision_attrs.items():
                    # Only update if the vision model provided a non-empty/non-default value
                    if value and value not in ['unknown', 'Unknown', 'N/A', 'n/a', 'none', 'None', []]:
                        # Standardize 'materials' to 'material'
                        if key == 'materials':
                            key = 'material'
                        
                        # For lists (like colors), extend the list
                        if isinstance(value, list) and key == 'colors':
                            current_colors = set(enrichment['attributes'].get('colors', []))
                            new_colors = [c for c in value if c not in current_colors]
                            enrichment['attributes']['colors'].extend(new_colors)
                        # For single values, update if current is default or new value is better
                        elif enrichment['attributes'].get(key) in ['unknown', 'Unknown', None] or key != 'colors':
                            enrichment['attributes'][key] = value
                            
            except Exception as e:
                print(f"Error processing image {img_url}: {e}")
        
        # Process text
        text_result = await self._process_text(product.get('description', ''))
        
        # Merge text attributes (text model is good for things like size/care)
        for key, value in text_result.items():
            if value and value not in ['unknown', 'Unknown', 'N/A', 'n/a', 'none', 'None', []]:
                # Standardize 'materials' to 'material'
                if key == 'materials':
                    key = 'material'
                
                # For lists (like colors), extend the list
                if isinstance(value, list) and key == 'colors':
                    current_colors = set(enrichment['attributes'].get('colors', []))
                    new_colors = [c for c in value if c not in current_colors]
                    enrichment['attributes']['colors'].extend(new_colors)
                # For single values, update if current is default or new value is better
                elif enrichment['attributes'].get(key) in ['unknown', 'Unknown', None] or key != 'colors':
                    enrichment['attributes'][key] = value
                    
        # Clean up colors list to be unique
        if 'colors' in enrichment['attributes']:
            enrichment['attributes']['colors'] = list(set(enrichment['attributes']['colors']))

        # FINAL FALLBACK: if AI calls failed or returned almost nothing,
        # derive lightweight attributes from title/description without LLM API calls.
        attrs = enrichment['attributes']
        if (
            attrs.get('category') in ['unknown', None]
            and not attrs.get('colors')
            and attrs.get('material') in ['unknown', None]
        ):
            title = (product.get('title') or '').lower()
            desc = (product.get('description') or '').lower()
            text = f"{title} {desc}"

            # Simple color extraction
            basic_colors = [
                'black', 'white', 'red', 'blue', 'green',
                'yellow', 'pink', 'purple', 'silver', 'gold',
                'brown', 'beige', 'grey', 'gray', 'orange',
            ]
            found_colors = sorted({c for c in basic_colors if c in text})
            if found_colors:
                attrs['colors'] = found_colors

            # Simple material extraction
            materials = ['gold', 'silver', 'diamond', 'leather', 'cotton', 'metal', 'steel', 'wood']
            for mat in materials:
                if mat in text:
                    attrs['material'] = mat
                    break

            # Category heuristic from existing product metadata or keywords
            if attrs.get('category') in ['unknown', None]:
                if product.get('category'):
                    attrs['category'] = product['category']
                elif any(k in text for k in ['ring', 'necklace', 'earring', 'bracelet']):
                    attrs['category'] = 'jewellery'
                elif any(k in text for k in ['shirt', 'dress', 'jeans', 't-shirt', 'hoodie']):
                    attrs['category'] = 'clothing'
        
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
    
    async def embed_text(self, text: str) -> List[float]:
        """Create embedding for semantic search (SOW 2.6).
        Using sentence-transformers for local embeddings (no API required).
        """
        if not text:
            return []
        try:
            # Use sentence-transformers for local embeddings (no API dependency)
            from sentence_transformers import SentenceTransformer
            
            # Lazy load the model (cache it for performance)
            if not hasattr(self, '_embedding_model'):
                self._embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            
            # Generate embedding synchronously (sentence-transformers is sync)
            embedding = self._embedding_model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        except ImportError:
            print("sentence-transformers not installed. Install with: pip install sentence-transformers")
            return []
        except Exception as e:
            print(f"Embedding error: {e}")
            return []
    
    async def _process_image(self, image_url: str) -> Dict:
        """SOW 2.5.A - Vision enrichment
        Note: Groq doesn't support vision models. Using text-based analysis of image URL/metadata.
        For full vision capabilities, consider using vision-capable APIs or services.
        """
        try:
            # Groq doesn't support vision, so we'll use text-based analysis
            # You can describe the image URL or use a vision API service
            client = Groq(api_key=os.getenv('GROQ_API_KEY'))
            
            # Create a prompt that asks the model to infer attributes from image URL/context
            prompt = f"""
            Based on the product image URL: {image_url}
            Analyze and provide:
            1. A detailed caption describing what might be in this product image (1-2 sentences)
            2. Extract structured attributes in JSON:
            {{
                "category": "clothing/electronics/furniture/etc",
                "colors": ["color1", "color2"],
                "material": "cotton/leather/metal/etc",
                "pattern": "solid/striped/floral/etc",
                "condition": "new/used/refurbished",
                "style": "casual/formal/sporty/etc",
                "occasion": "casual/office/party/gym/etc"
            }}
            
            Return JSON with "caption" and "attributes" keys.
            """
            
            response = client.chat.completions.create(
                model="llama-3.1-70b-versatile",  # Groq's fast model
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=500,
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content
            
            # Parse JSON from response
            json_match = result_text.find('{')
            if json_match >= 0:
                json_str = result_text[json_match:]
                json_end = json_str.rfind('}') + 1
                try:
                    attributes = json.loads(json_str[:json_end])
                    caption = result_text[:json_match].strip()
                except json.JSONDecodeError:
                    # If JSON parsing fails, try to extract attributes from text
                    caption = result_text
                    attributes = {}
            else:
                caption = result_text
                attributes = {}
            
            return {
                'image_url': image_url,
                'caption': caption or 'Image processed',
                'attributes': attributes
            }
        
        except Exception as e:
            print(f"Groq image processing error: {e}")
            return {
                'image_url': image_url,
                'caption': 'Image processed',
                'attributes': {}
            }
    
    async def _process_text(self, description: str) -> Dict:
        """SOW 2.5.B - Text enrichment using Groq"""
        if not description:
            return {}
        
        try:
            client = Groq(api_key=os.getenv('GROQ_API_KEY'))
            response = client.chat.completions.create(
                model="llama-3.1-70b-versatile",  # Groq's fast and capable model
                messages=[
                    {
                        "role": "user",
                        "content": f"""
                        Extract structured attributes from this product description:
                        
                        {description}
                        
                        Return JSON with fields: category, material, colors, pattern, size_range, care_instructions
                        Keep unknown fields as null or "unknown".
                        """
                    }
                ],
                max_tokens=300,
                temperature=0.3
            )
            
            result_text = response.choices[0].message.content
            json_start = result_text.find('{')
            if json_start >= 0:
                try:
                    attributes = json.loads(result_text[json_start:])
                    return attributes
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON from Groq response: {result_text}")
        except Exception as e:
            print(f"Groq text processing error: {e}")
        
        return {}
