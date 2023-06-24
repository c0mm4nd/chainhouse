mod serde_bytes_array {
    use core::convert::TryInto;

    use serde::de::Error;
    use serde::{Deserializer, Serializer};

    /// This just specializes [`serde_bytes::serialize`] to `<T = [u8]>`.
    pub(crate) fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(bytes, serializer)
    }

    /// This takes the result of [`serde_bytes::deserialize`] from `[u8]` to `[u8; N]`.
    pub(crate) fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        let slice: &[u8] = serde_bytes::deserialize(deserializer)?;
        let array: [u8; N] = slice.try_into().map_err(|_| {
            let expected = format!("[u8; {}]", N);
            D::Error::invalid_length(slice.len(), &expected.as_str())
        })?;
        Ok(array)
    }
}


macro_rules! option {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        pub mod option {
            use super::*;

            struct $name(super::$name);

            impl Serialize for $name {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    super::serialize(&self.0, serializer)
                }
            }

            impl<'de> Deserialize<'de> for $name {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    super::deserialize(deserializer).map($name)
                }
            }

            pub fn serialize<S>(v: &Option<super::$name>, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                v.clone().map($name).serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<super::$name>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let opt: Option<$name> = Deserialize::deserialize(deserializer)?;
                Ok(opt.map(|v| v.0))
            }
        }
    };
}

pub mod u64 {
    use ethers::types::U64;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(U64, "Ser/de `Option<U64>` to/from `Nullable(U64)`.");

    pub fn serialize<S: Serializer>(u: &U64, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u64; 1] = Deserialize::deserialize(deserializer)?;
        Ok(U64(u))
    }
}

pub mod h64 {
    use ethers::types::H64;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(H64, "Ser/de `Option<H64>` to/from `Nullable(H64)`.");

    pub fn serialize<S: Serializer>(u: &H64, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u8; 8] = Deserialize::deserialize(deserializer)?;
        Ok(H64(u))
    }
}

pub mod u256 {
    use ethers::types::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(U256, "Ser/de `Option<U256>` to/from `Nullable(U256)`.");

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u64; 4] = Deserialize::deserialize(deserializer)?;
        Ok(U256(u))
    }
}

pub mod h160 {
    use ethers::types::H160;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(H160, "Ser/de `Option<H160>` to/from `Nullable(H160)`.");

    pub fn serialize<S: Serializer>(u: &H160, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H160, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u = Deserialize::deserialize(deserializer)?;
        Ok(H160(u))
    }
}

pub mod h256 {
    use ethers::types::H256;
    use serde::{
        de::{Deserialize, Deserializer, Error},
        ser::{Serialize, Serializer},
    };

    option!(H256, "Ser/de `Option<H256>` to/from `Nullable(H256)`.");

    pub fn serialize<S: Serializer>(u: &H256, serializer: S) -> Result<S::Ok, S::Error> {
        // u.0.serialize(serializer)
        println!("{:?}", &u.as_bytes());
        
        serializer.serialize_bytes(u.as_bytes())
        // serde_bytes::serialize_str(&u.as_bytes(), serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<H256, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let u:[u8; 32] = serde_bytes::deserialize(deserializer)?;
        // Ok(H256(u))

        let slice: &[u8] = serde_bytes::deserialize(deserializer)?;
        let array: [u8; 32] = slice.try_into().map_err(|_| {
            let expected = format!("[u8; {}]", 32);
            D::Error::invalid_length(slice.len(), &expected.as_str())
        })?;
        // Ok(array)

        Ok(H256(array))
    }
}

pub mod access_list {
    use ethers::types::transaction::eip2930::{AccessList, AccessListItem};
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    option!(
        AccessList,
        "Ser/de `Option<AccessList>` to/from `Nullable(AccessList)`."
    );

    pub fn serialize<S: Serializer>(u: &AccessList, serializer: S) -> Result<S::Ok, S::Error> {
        let raw = serde_json::to_string(u).unwrap();
        raw.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<AccessList, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: String = Deserialize::deserialize(deserializer)?;
        let al: AccessList = serde_json::from_str(&u).unwrap();
        Ok(al)
    }
}
