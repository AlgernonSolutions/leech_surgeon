def check_for_hotwords(question_text, hotwords):
    results = []
    for word in hotwords:
        if word in question_text:
            results.append(word)
    return results
