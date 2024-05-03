const BASE_URL: &str = "https://www.hydrovu.com";

fn combine(a: &str, b: &str) -> String {
    return format!("{a}/public-api/{b}");
}

pub fn names_url() -> String {
    return combine(BASE_URL, "v1/sispec/friendlynames");
}

pub fn locations_url() -> String {
    return combine(BASE_URL, "v1/locations/list");
}

pub fn location_url(id: i64, start_time: i64, end_time: i64) -> String {
    return combine(
        BASE_URL,
        format!("v1/locations/{id}/data?startTime={start_time}&endTime={end_time}").as_str(),
    );
}

pub fn auth_url() -> String {
    return combine(BASE_URL, "oauth/authorize");
}

pub fn token_url() -> String {
    return combine(BASE_URL, "oauth/token");
}
