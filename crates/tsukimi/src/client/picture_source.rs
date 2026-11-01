use gtk::glib;

#[derive(Clone, glib::Boxed)]
#[boxed_type(name = "TsukimiPictureSource", nullable)]
pub enum PictureSource {
    Item {
        id: String,
        tag: String,
        image_type: &'static str,
        image_index: Option<u8>,
    },
    User {
        id: String,
        tag: String,
    },
    Url {
        url: String,
    },
}

impl PictureSource {
    pub fn cache_key(&self) -> String {
        match self {
            Self::Item {
                id,
                tag,
                image_type,
                image_index,
            } => format!("{}-{}-{}-{}", id, image_type, image_index.unwrap_or(0), tag),
            Self::User { id, tag } => format!("{id}-Primary-0-{tag}"),
            Self::Url { .. } => unreachable!(),
        }
    }
}

/// Requested pixel size of one image.
///
/// Servers resize on the fly, so asking for the wrong size is wasteful in both
/// directions: 300 px posters look soft on HiDPI screens, while requesting the
/// original upload wastes bandwidth. These are the logical targets; multiply by
/// the widget's scale factor before sending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageTarget {
    pub width: u32,
    pub height: u32,
}

impl ImageTarget {
    /// Grid cards and list thumbnails.
    pub const CARD: Self = Self {
        width: 300,
        height: 450,
    };
    /// Full width page backdrops.
    pub const BACKDROP: Self = Self {
        width: 1280,
        height: 800,
    };
    /// Small round avatars.
    pub const AVATAR: Self = Self {
        width: 50,
        height: 50,
    };

    /// Resize for the physical pixel ratio. Capped so a 4K screen does not ask
    /// for 8K masters.
    pub fn scaled(self, scale: u32) -> Self {
        let factor = scale.clamp(1, 3);
        Self {
            width: (self.width * factor).min(2048),
            height: (self.height * factor).min(2048),
        }
    }

    pub fn cache_suffix(self) -> String {
        format!("{}x{}", self.width, self.height)
    }
}

#[cfg(test)]
mod tests {
    use super::ImageTarget;

    #[test]
    fn size_scales_with_the_pixel_ratio_but_is_capped() {
        assert_eq!(ImageTarget::CARD.scaled(1).width, 300);
        assert_eq!(ImageTarget::CARD.scaled(2).width, 600);
        // A broken scale factor must not produce a 0 px request.
        assert_eq!(ImageTarget::CARD.scaled(0).width, 300);
        // …and a 3x request must stay sane.
        assert!(ImageTarget::BACKDROP.scaled(3).width <= 2048);
    }

    #[test]
    fn cache_suffix_encodes_the_resolved_size() {
        assert_eq!(ImageTarget::CARD.scaled(2).cache_suffix(), "600x900");
        assert_ne!(
            ImageTarget::CARD.scaled(1).cache_suffix(),
            ImageTarget::CARD.scaled(2).cache_suffix()
        );
    }
}
