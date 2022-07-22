def split_paragraphs(text, css_style):
    list = text.split('. ')
    result = []
    paragraph = ''
    for sentence in list:
        paragraph += sentence + '. '
        if len(paragraph) > 200:
            result.append(paragraph)
            paragraph = ''
    delimiter = '</%s><%s>' % (css_style, css_style)
    return delimiter.join(result)
