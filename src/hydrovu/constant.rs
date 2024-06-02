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

pub fn location_url(id: i64, start_time: i64, end_time_opt: Option<i64>) -> String {
    let mut end_str: String = "".to_string();
    if let Some(end_time) = end_time_opt {
	end_str = format!("&endTime={end_time}");
    }
    combine(
        BASE_URL,
        format!("v1/locations/{id}/data?startTime={start_time}{end_str}").as_str(),
    )
}

pub fn auth_url() -> String {
    return combine(BASE_URL, "oauth/authorize");
}

pub fn token_url() -> String {
    return combine(BASE_URL, "oauth/token");
}
