package shared

import (
    "os"

    "strings"
    "strconv"

    "github.com/hashicorp/go-hclog"
)

// DisplayFilteredEnv shows filtered environment variables if the PLUGIN_SHOW_ENV is enabled.
// - `logger`: Logger instance for logging environment variables.
// - `defaultFilter`: A default filter applied if PLUGIN_ENV_FILTER is not set.
func DisplayFilteredEnv(logger hclog.Logger, defaultFilter []string) {

       // Default showing the environment variables to off.
    showEnv := false
    showEnvValue := os.Getenv("PLUGIN_SHOW_ENV")
    if showEnvValue != "" {
        showEnv, _ = strconv.ParseBool(strings.ToLower(showEnvValue))

        if !showEnv {
            logger.Debug("🔕 Environment variable display is disabled.")
            return
        }
    }

    // Retrieve filter list from PLUGIN_ENV_FILTER or use the default
    rawFilter := os.Getenv("PLUGIN_ENV_FILTER")
    var filters []string
    if rawFilter != "" {
        filters = strings.Split(rawFilter, ",")
    } else {
        filters = defaultFilter
    }

    logger.Info("📡🌍 Displaying Environment Variables:")
    for _, env := range os.Environ() {
        keyValue := strings.SplitN(env, "=", 2)
        key := keyValue[0]

        // Check if the key matches any filter in the list
        for _, filter := range filters {
            if strings.Contains(key, filter) {
                logger.Info("🔑 " + key + "=" + keyValue[1])
                break
            }
        }
    }
}