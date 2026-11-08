{-# LANGUAGE OverloadedStrings #-}
-- | Which build this is: the APK's build puts a hash of everything it is
-- made of here, to tell builds apart.
module HTox.Version (version) where

import           Data.Text (Text)

version :: Text
version = "dev"
