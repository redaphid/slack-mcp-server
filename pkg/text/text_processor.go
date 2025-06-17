package text

import (
	"regexp"
	"strings"
)

func ProcessText(s string) string {
	s = filterSpecialChars(s)

	return s
}

func filterSpecialChars(text string) string {
	replaceWithCommaCheck := func(match []string, isLast bool) string {
		var url, linkText string

		if len(match) == 3 && strings.Contains(match[0], "|") {
			url = match[1]
			linkText = match[2]
		} else if len(match) == 3 {
			linkText = match[1]
			url = match[2]
		}

		replacement := url + " - " + linkText

		if !isLast {
			replacement += ","
		}

		return replacement
	}

	// Helper function to check if this is the last link/element
	isLastInText := func(original string, currentText string) bool {
		linkPos := strings.LastIndex(currentText, original)
		if linkPos == -1 {
			return false
		}
		afterLink := strings.TrimSpace(currentText[linkPos+len(original):])
		return afterLink == ""
	}

	// Handle Slack-style links: <URL|Description>
	slackLinkRegex := regexp.MustCompile(`<(https?://[^>|]+)\|([^>]+)>`)
	slackMatches := slackLinkRegex.FindAllStringSubmatch(text, -1)
	for _, match := range slackMatches {
		original := match[0]
		isLast := isLastInText(original, text)
		replacement := replaceWithCommaCheck(match, isLast)
		text = strings.Replace(text, original, replacement, 1)
	}

	// Handle markdown links: [Description](URL)
	markdownLinkRegex := regexp.MustCompile(`\[([^\]]+)\]\((https?://[^)]+)\)`)
	markdownMatches := markdownLinkRegex.FindAllStringSubmatch(text, -1)
	for _, match := range markdownMatches {
		original := match[0]
		isLast := isLastInText(original, text)
		replacement := replaceWithCommaCheck(match, isLast)
		text = strings.Replace(text, original, replacement, 1)
	}

	htmlLinkRegex := regexp.MustCompile(`<a\s+href=["']([^"']+)["'][^>]*>([^<]+)</a>`)
	htmlMatches := htmlLinkRegex.FindAllStringSubmatch(text, -1)
	for _, match := range htmlMatches {
		original := match[0]
		isLast := isLastInText(original, text)
		url := match[1]
		linkText := match[2]
		replacement := url + " - " + linkText
		if !isLast {
			replacement += ","
		}
		text = strings.Replace(text, original, replacement, 1)
	}

	urlRegex := regexp.MustCompile(`https?://[^\s<>"{}|\\^` + "`" + `\[\]]+`)
	urls := urlRegex.FindAllString(text, -1)

	protected := text
	for i, url := range urls {
		placeholder := "___URL_PLACEHOLDER_" + string(rune(48+i)) + "___"
		protected = strings.Replace(protected, url, placeholder, 1)
	}

	cleanRegex := regexp.MustCompile(`[^0-9\p{L}\p{M}\s\.\,\-_:/\?=&%]`)
	cleaned := cleanRegex.ReplaceAllString(protected, "")

	// Restore the URLs
	for i, url := range urls {
		placeholder := "___URL_PLACEHOLDER_" + string(rune(48+i)) + "___"
		cleaned = strings.Replace(cleaned, placeholder, url, 1)
	}

	spaceRegex := regexp.MustCompile(`\s+`)
	cleaned = spaceRegex.ReplaceAllString(cleaned, " ")

	return strings.TrimSpace(cleaned)
}
