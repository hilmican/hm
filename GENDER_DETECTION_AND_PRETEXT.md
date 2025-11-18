# Gender Detection and Pretext System Implementation

## Problem Solved

The AI was incorrectly using "abim" (brother) for all customers, including women. This implementation adds:
1. **Gender detection** based on customer information (username, name, contact_name)
2. **Pretext system** for customizable per-product system message prefixes
3. **Proper greeting selection**: "abim" (male), "ablam" (female), or "efendim" (unknown/formal)

## Changes Made

### 1. Database Schema

#### New Table: `ai_pretext`
- Stores pretext templates that can be prepended to AI system messages
- Fields:
  - `id`: Primary key
  - `name`: Pretext identifier/name
  - `content`: The pretext text (LONGTEXT)
  - `is_default`: Boolean flag for default pretext
  - `created_at`, `updated_at`: Timestamps

#### Product Table: New Field `pretext_id`
- Links a product to a specific pretext
- If `NULL`, uses the default pretext (first one marked as default, or first one)

### 2. Code Changes

#### `app/models.py`
- Added `AIPretext` model class
- Added `pretext_id` field to `Product` model

#### `app/db.py`
- Added migration code to create `ai_pretext` table
- Added migration code to add `pretext_id` column to `product` table

#### `app/services/ai_reply.py`
- Added `_load_customer_info()` function to fetch customer data from `IGUser`
- Modified `draft_reply()` to:
  - Load customer information (username, name, contact_name)
  - Load and apply pretext (product-specific or default)
  - Generate gender detection instructions
  - Combine pretext + gender instructions + product system message

## How It Works

### System Prompt Structure

The final system prompt is built in this order:
1. **Pretext** (if available) - Customizable per-product prefix
2. **Gender Detection Instructions** - Always included with customer info
3. **Product System Message** (if available) - Existing `ai_system_msg` from product

### Gender Detection Logic

The AI receives customer information and instructions to:
- Use **"abim"** for male customers
- Use **"ablam"** for female customers  
- Use **"efendim"** when gender cannot be determined

Detection criteria:
- Turkish name patterns (e.g., "-a", "-e" endings often indicate female names)
- Common Turkish names (Ayşe, Fatma, Zeynep = female; Mehmet, Ali, Ahmet = male)
- Falls back to "efendim" when uncertain

### Pretext Selection Logic

1. If product has `pretext_id` set → Use that specific pretext
2. If product has no `pretext_id` → Use default pretext (marked with `is_default=True`)
3. If no default pretext exists → Use first pretext (by ID)
4. If no product focus → Use default/first pretext

## Usage

### Creating Pretexts

You'll need to insert pretexts into the database. Example SQL:

```sql
-- Create a default pretext
INSERT INTO ai_pretext (name, content, is_default) VALUES (
  'Default',
  'Sen HiMan için Instagram DM satış asistanısın. Amacın, müşteriyi nazik ve samimi bir dille hızlıca doğru bedene yönlendirip siparişe dönüştürmek.',
  1
);

-- Create another pretext for a specific product line
INSERT INTO ai_pretext (name, content, is_default) VALUES (
  'Premium Line',
  'Sen HiMan Premium ürünleri için özel satış asistanısın. Premium kalite ve özel hizmet vurgusu yap.',
  0
);
```

### Assigning Pretext to Product

```sql
-- Set a product to use a specific pretext
UPDATE product SET pretext_id = 2 WHERE id = 123;

-- Or set to NULL to use default
UPDATE product SET pretext_id = NULL WHERE id = 123;
```

### Admin Interface (Future Enhancement)

You may want to create an admin interface to:
- List/create/edit/delete pretexts
- Set default pretext
- Assign pretexts to products via UI

## Testing

To test the implementation:

1. **Create a test pretext** in the database
2. **Assign it to a product** (or leave NULL for default)
3. **Ensure customer info exists** in `ig_users` table (username, name, contact_name)
4. **Generate an AI reply** and check the system prompt includes:
   - The pretext (if assigned)
   - Gender detection instructions with customer info
   - Product system message (if exists)
5. **Verify the AI uses correct greeting** (abim/ablam/efendim)

## Example System Prompt

```
[Pretext content if exists]

## Müşteri Hitap Kuralları

Müşteri bilgileri:
- Kullanıcı adı: zeynep_customer
- İsim: Zeynep Yılmaz
- İletişim adı: Zeynep Yılmaz

HITAP KURALLARI:
1. Müşterinin cinsiyetini belirlemek için yukarıdaki bilgileri kullan.
2. Eğer müşteri ERKEK ise: "abim" kullan
3. Eğer müşteri KADIN ise: "ablam" kullan
4. Eğer cinsiyeti belirleyemiyorsan: "efendim" kullan
...

[Product ai_system_msg if exists]
```

## Notes

- The gender detection is based on Turkish name patterns and common names
- If customer info is missing, all fields show "bilinmiyor" and AI should use "efendim"
- The pretext system allows for flexible customization per product
- Default pretext ensures there's always a pretext available even if none is assigned

