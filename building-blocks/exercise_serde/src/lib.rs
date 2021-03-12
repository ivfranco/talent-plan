use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum Dir {
    N,
    W,
    S,
    E,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Move {
    dir: Dir,
    distance: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{
        distributions::{Distribution, Standard},
        Rng,
    };
    use std::{
        error::Error,
        io::{Cursor, Seek, SeekFrom},
        str::from_utf8,
    };
    use tempfile::tempfile;

    impl Distribution<Dir> for Standard {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Dir {
            match rng.gen_range(0..4) {
                0 => Dir::N,
                1 => Dir::W,
                2 => Dir::S,
                3 => Dir::E,
                _ => unreachable!(),
            }
        }
    }

    impl Distribution<Move> for Standard {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Move {
            Move {
                dir: rng.gen(),
                distance: rng.gen(),
            }
        }
    }

    #[test]
    fn ser_de_json() -> Result<(), Box<dyn Error>> {
        let mut file = tempfile()?;
        let a: Move = rand::random();
        serde_json::to_writer(&file, &a)?;
        file.seek(SeekFrom::Start(0))?;
        let b: Move = serde_json::from_reader(file)?;
        assert_eq!(a, b);
        Ok(())
    }

    #[test]
    fn ser_de_ron() -> Result<(), Box<dyn Error>> {
        let mut vec = vec![];
        let a: Move = rand::random();
        ron::ser::to_writer(&mut vec, &a)?;
        println!("{}", from_utf8(&vec).unwrap());
        let b: Move = ron::de::from_bytes(&vec)?;
        assert_eq!(a, b);
        Ok(())
    }

    #[test]
    fn ser_de_bson() -> Result<(), Box<dyn Error>> {
        let rng = rand::thread_rng();
        let ser_moves: Vec<Move> = rng.sample_iter(Standard).take(1000).collect();

        {
            let mut file = tempfile()?;
            for m in &ser_moves {
                let bson = bson::to_document(&m)?;
                bson.to_writer(&mut file)?;
            }
            file.seek(SeekFrom::Start(0))?;
            // the function from_reader doesn't require a Seek or BufRead impl
            // the deserializer maintains the correct file offsets by reading just enough bytes
            let de_moves = (0..ser_moves.len())
                .map(|_| bson::Document::from_reader(&mut file).and_then(bson::from_document))
                .collect::<Result<Vec<Move>, _>>()?;

            assert_eq!(ser_moves, de_moves);
        }

        {
            let mut vec: Vec<u8> = vec![];
            for m in &ser_moves {
                let bson = bson::to_document(&m)?;
                bson.to_writer(&mut vec)?;
            }

            let mut cursor = Cursor::new(&vec);

            let de_moves = (0..ser_moves.len())
                .map(|_| bson::Document::from_reader(&mut cursor).and_then(bson::from_document))
                .collect::<Result<Vec<Move>, _>>()?;

            assert_eq!(ser_moves, de_moves);
        }

        Ok(())
    }
}
