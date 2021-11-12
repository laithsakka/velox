/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include "velox/core/CoreTypeSystem.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::exec {
template <typename T, typename U>
struct VectorReader;

// Implements an iterator for T that moves by calling incrementIndex(). T must
// implement index() and incrementIndex(). Two iterators from the same
// "container" points to the same element if they have the same index.
template <typename T>
class IndexBasedIterator
    : public std::iterator<std::input_iterator_tag, T, size_t> {
 public:
  using Iterator = IndexBasedIterator<T>;

  explicit IndexBasedIterator<T>(const T& element) : element_(element) {}

  bool operator!=(const Iterator& rhs) const {
    return element_.index() != rhs.element_.index();
  }

  bool operator==(const Iterator& rhs) const {
    return element_.index() == rhs.element_.index();
  }

  const T& operator*() const {
    return element_;
  }

  const T* operator->() const {
    return &element_;
  }

  bool operator<(const Iterator& rhs) const {
    return this->element_.index() < rhs.element_.index();
  }

  // Implement post increment.
  Iterator operator++(int) {
    Iterator old = *this;
    ++*this;
    return old;
  }

  // Implement pre increment.
  Iterator& operator++() {
    element_.incrementIndex();
    return *this;
  }

 protected:
  T element_;
};

// Given a vectorReader T, this class represents a lazy access optional wrapper
// around an element in the vectorReader with interface similar to
// std::optional<T::exec_in_t>. This is used to represent elements of ArrayView
// and values of MapView. OptionalVectorValueAccessor can be compared with and
// assigned to std::optional.
template <typename T>
class OptionalVectorValueAccessor {
 public:
  using element_t = typename T::exec_in_t;

  OptionalVectorValueAccessor<T>(const T* reader, vector_size_t index)
      : reader_(reader), index_(index) {}

  operator bool() const {
    return this->has_value();
  }

  // Enable to be assigned to std::optional<element_t>.
  operator std::optional<element_t>() const {
    if (!this->has_value()) {
      return std::nullopt;
    }
    return {value()};
  }

  // Disable all other implicit casts to avid odd behaviours.
  template <typename B>
  operator B() const = delete;

  bool operator==(const OptionalVectorValueAccessor& other) const {
    if (other.has_value() != this->has_value()) {
      return false;
    }

    if (this->has_value()) {
      return this->value() == other.value();
    }
    // Both are nulls.
    return true;
  }

  bool operator!=(const OptionalVectorValueAccessor& other) const {
    return !(*this == other);
  }

  bool has_value() const {
    return reader_->isSet(index_);
  }

  element_t value() const {
    DCHECK(has_value());
    return (*reader_)[index_];
  }

  element_t operator*() const {
    DCHECK(has_value());
    return (*reader_)[index_];
  }

  void incrementIndex() {
    index_++;
  }

  vector_size_t index() const {
    return index_;
  }

 private:
  const T* reader_;
  // Index of element within the reader.
  vector_size_t index_;
};

// Allow comparing OptionalVectorValueAccessor with std::optional.
template <typename T, typename U>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator==(
    const std::optional<T>& lhs,
    const OptionalVectorValueAccessor<U>& rhs) {
  if (lhs.has_value() != rhs.has_value()) {
    return false;
  }

  if (lhs.has_value()) {
    return lhs.value() == rhs.value();
  }
  // Both are nulls.
  return true;
}

template <typename U, typename T>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator==(
    const OptionalVectorValueAccessor<U>& lhs,
    const std::optional<T>& rhs) {
  return rhs == lhs;
}

template <typename T, typename U>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator!=(
    const std::optional<T>& lhs,
    const OptionalVectorValueAccessor<U>& rhs) {
  return !(lhs == rhs);
}

template <typename U, typename T>
typename std::enable_if<
    std::is_trivially_constructible<typename U::exec_in_t, T>::value,
    bool>::type
operator!=(
    const OptionalVectorValueAccessor<U>& lhs,
    const std::optional<T>& rhs) {
  return !(lhs == rhs);
}

// Represents an array of elements with an interface similar to std::vector.
template <typename V>
class ArrayView {
  using reader_t = VectorReader<V, void>;
  using element_t = typename reader_t::exec_in_t;

 public:
  ArrayView(const reader_t* reader, vector_size_t offset, vector_size_t size)
      : reader_(reader), offset_(offset), size_(size) {}

  // The previous doLoad protocol creates a value and then assigns to it.
  // TODO: this should deprecated once we deprecate the doLoad protocol.
  ArrayView() : reader_(nullptr), offset_(0), size_(0) {}

  using Element = OptionalVectorValueAccessor<reader_t>;
  using Iterator = IndexBasedIterator<Element>;

  Iterator begin() const {
    return Iterator{Element{reader_, offset_}};
  }

  Iterator end() const {
    return Iterator{Element{reader_, offset_ + size_}};
  }

  // Returns true if any of the arrayViews in the vector might have null
  // element.
  bool mayHaveNulls() const {
    return false;
  }

  Element operator[](vector_size_t index) const {
    return Element{reader_, index + offset_};
  }

  Element at(vector_size_t index) const {
    return (*this)[index];
  }

  size_t size() const {
    return size_;
  }

 private:
  const reader_t* reader_;
  vector_size_t offset_;
  vector_size_t size_;
};

// This class is used to represent map inputs in simple functions with an
// interface similar to std::map.
template <typename K, typename V>
class MapView {
 public:
  using key_reader_t = VectorReader<K, void>;
  using value_reader_t = VectorReader<V, void>;
  using key_element_t = typename key_reader_t::exec_in_t;

  MapView(
      const key_reader_t* keyReader,
      const value_reader_t* valueReader,
      vector_size_t offset,
      vector_size_t size)
      : keyReader_(keyReader),
        valueReader_(valueReader),
        offset_(offset),
        size_(size) {}

  MapView()
      : keyReader_(nullptr), valueReader_(nullptr), offset_(0), size_(0) {}

  // This class represents a lazy access wrapper around the key.
  struct LazyKeyAccessor {
    LazyKeyAccessor(const key_reader_t* reader, vector_size_t index)
        : reader_(reader), index_(index) {}

    operator key_element_t() const {
      return (*reader_)[index_];
    }

    bool operator==(const LazyKeyAccessor& other) const {
      return key_element_t(other) == key_element_t(*this);
    }

    vector_size_t index() const {
      return index_;
    }

    void incrementIndex() {
      index_++;
    }

   private:
    const key_reader_t* reader_;
    vector_size_t index_;
  };

  class Element {
   public:
    Element(
        const key_reader_t* keyReader,
        const value_reader_t* valueReader,
        vector_size_t index)
        : first(keyReader, index), second(valueReader, index), index_(index) {}
    LazyKeyAccessor first;
    OptionalVectorValueAccessor<value_reader_t> second;

    bool operator==(const Element& other) const {
      return first == other.first && second == other.second;
    }

    // T is pair like object.
    template <typename T>
    bool operator==(const T& other) const {
      return first == other.first && second == other.second;
    }

    template <typename T>
    bool operator!=(const T& other) const {
      return !(*this == other);
    }

    void incrementIndex() {
      index_++;
      first.incrementIndex();
      second.incrementIndex();
    }

    vector_size_t index() const {
      return index_;
    }

   private:
    vector_size_t index_;
  };

  using Iterator = IndexBasedIterator<Element>;

  Iterator begin() const {
    return Iterator{Element{keyReader_, valueReader_, 0 + offset_}};
  }

  Iterator end() const {
    return Iterator{Element{keyReader_, valueReader_, size_ + offset_}};
  }

  const Element operator[](vector_size_t index) const {
    return Element{keyReader_, valueReader_, index + offset_};
  }

  size_t size() const {
    return size_;
  }

 private:
  const key_reader_t* keyReader_;
  const value_reader_t* valueReader_;
  vector_size_t offset_;
  vector_size_t size_;
};
} // namespace facebook::velox::exec