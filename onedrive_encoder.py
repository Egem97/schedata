import base64


def encode_onedrive_sharing_url(sharing_url):
    """
    Encodes a OneDrive sharing URL to the format used by Microsoft Graph API.
    
    Args:
        sharing_url (str): The OneDrive sharing URL to encode
        
    Returns:
        str: The encoded URL in the format "u!<base64_encoded_url>"
        
    Example:
        >>> url = "https://onedrive.live.com/redir?resid=1231244193912!12&authKey=1201919!12921!1"
        >>> encoded = encode_onedrive_sharing_url(url)
        >>> print(encoded)
    """
    # Convert string to bytes using UTF-8 encoding
    url_bytes = sharing_url.encode('utf-8')
    
    # Convert to base64
    base64_value = base64.b64encode(url_bytes).decode('utf-8')
    
    # Remove padding '=' characters and replace URL-unsafe characters
    encoded_url = "u!" + base64_value.rstrip('=').replace('/', '_').replace('+', '-')
    
    return encoded_url


# Example usage
if __name__ == "__main__":
    sharing_url = "https://onedrive.live.com/redir?resid=1231244193912!12&authKey=1201919!12921!1"
    encoded_url = encode_onedrive_sharing_url(sharing_url)
    print(f"Original URL: {sharing_url}")
    print(f"Encoded URL: {encoded_url}")