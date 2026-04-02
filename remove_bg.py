from PIL import Image
import numpy as np

def make_transparent(input_path, output_path):
    img = Image.open(input_path).convert('RGBA')
    data = np.array(img)
    
    r, g, b, a = data[:,:,0], data[:,:,1], data[:,:,2], data[:,:,3]
    
    # Calculate luminance or just distance to the target dark background #0a1628 (10, 22, 40)
    bg_r, bg_g, bg_b = 10, 22, 40
    
    # Distance from background
    dist = np.sqrt((r.astype(float) - bg_r)**2 + (g.astype(float) - bg_g)**2 + (b.astype(float) - bg_b)**2)
    
    # Normalize distance to use as alpha mask
    # Low distance (close to bg) -> 0 alpha (transparent)
    # High distance (neon lines) -> 255 alpha (opaque)
    
    # Let's say any distance < 30 is fully transparent, > 100 is fully opaque
    mask = (dist - 15) / 85.0
    mask = np.clip(mask, 0, 1)
    
    # Apply mask to alpha channel
    data[:,:,3] = (a * mask).astype(np.uint8)
    
    # For the pixels that are semi-transparent, we should maybe boost their brightness 
    # so they don't look muddy, or just leave them.
    
    out_img = Image.fromarray(data)
    out_img.save(output_path)
    print("Background removed successfully.")

if __name__ == '__main__':
    make_transparent('bull_bear_banner_neon.png', 'bull_bear_banner_transparent.png')
